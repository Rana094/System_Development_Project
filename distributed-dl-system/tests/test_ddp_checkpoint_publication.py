"""9.3 complete-save validation and canonical publication in disposable stores.

Opaque rank members exercise the privileged metadata boundary, which MUST NOT
deserialize tensors. Native writer/resume correctness is tested by existing Gloo
suites. No real NFS, Pi, live DB, deployment or Internet runtime transfer.
"""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import hashlib
import io
import json
from pathlib import Path
import sys
import threading
import unittest
from unittest.mock import patch
import warnings
import zipfile

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import test_checkpoint_publication as fixture
from hybrid_storage import checkpoint_publication as publication
from checkpoint_protocol import validate_group


class DistributedSaveTests(unittest.TestCase):
    def setUp(self):
        self.f=fixture.PublicationTests(methodName='test_ddp_complete_three_rank_bundle_and_all_current_fences_required')
        self.addCleanup(self.f.doCleanups);self.f.setUp()
        self.db=self.f.db;self.publisher=self.f.publisher
        import recovery_metrics
        with patch.object(recovery_metrics,'DB_PATH',self.f.f.db_path):recovery_metrics.init_table()

    def rewrite(self,raw,*,metadata=None,members=None,duplicate=None):
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            entries={name:archive.read(name) for name in archive.namelist()}
        if metadata:
            info=json.loads(entries['metadata.json']);metadata(info)
            entries['metadata.json']=json.dumps(info).encode()
        if members:members(entries)
        result=io.BytesIO()
        with warnings.catch_warnings(),zipfile.ZipFile(result,'w',compression=zipfile.ZIP_STORED) as archive:
            warnings.simplefilter('ignore',UserWarning)  # Deliberately duplicate one ZIP member.
            for name,raw in entries.items():archive.writestr(name,raw)
            if duplicate:archive.writestr(duplicate,entries[duplicate])
        return result.getvalue()

    def state(self):
        f=self.f
        return (tuple(self.db.execute('SELECT publication_id,revision FROM hybrid_checkpoint_heads WHERE job_id=?',(f.job_id,)).fetchone()),
                tuple(self.db.execute('SELECT checkpoint_path,checkpoint_updated_at,checkpoint_size_bytes,updated_at FROM jobs WHERE job_id=?',(f.job_id,)).fetchone()),
                self.db.execute("SELECT COUNT(*) FROM recovery_events WHERE event_type='DDP_CHECKPOINT_COMMITTED'").fetchone()[0])

    def committed_count(self):
        return self.db.execute("SELECT COUNT(*) FROM hybrid_checkpoint_publications WHERE phase='COMMITTED'").fetchone()[0]

    def test_one_committed_manifest_preserves_all_rank_members_hashes_and_acks(self):
        f=self.f;raw=f.archive();receipt=f.publish(raw)
        target=f.root/'commits'/(receipt.publication_id+'.dlckpt')
        self.assertEqual(target.read_bytes(),raw)
        info=validate_group(target,f.job_id,f.job['storage_manifest_sha256'],f.job_id+':0')
        stored=json.loads(self.publisher._publication(receipt.publication_id)['metadata_json'])
        self.assertEqual(stored,info);self.assertEqual(stored['world_size'],3)
        self.assertEqual([ack['rank'] for ack in stored['rank_acknowledgments']],[0,1,2])
        self.assertEqual(set(stored['rank_state_sha256']),{'0','1','2'})
        self.assertEqual(set(stored['rank_fences']),{'0','1','2'})
        self.assertEqual(self.state()[0],(receipt.publication_id,1));self.assertEqual(self.state()[2],1)

    def test_fresh_transfer_id_for_same_save_returns_canonical_without_head_or_event_change(self):
        f=self.f;raw=f.archive();first=f.publish(raw);before=self.state()
        duplicate=f.publish(raw,identity='2'*32,head=first.publication_id)
        self.assertEqual(duplicate,first);self.assertEqual(self.state(),before)
        self.assertEqual(self.committed_count(),1)
        self.assertEqual(self.publisher._publication('2'*32)['phase'],'REJECTED')
        self.assertEqual(len(list((f.root/'commits').iterdir())),1)

    def test_conflicting_bytes_cannot_reuse_checkpoint_id(self):
        f=self.f;raw=f.archive();first=f.publish(raw);before=self.state()
        changed=self.rewrite(raw,metadata=lambda meta:meta.update(created_at=2))
        with self.assertRaises(publication.base.RegistryConflict):
            f.publish(changed,identity='2'*32,head=first.publication_id)
        self.assertEqual(self.state(),before);self.assertEqual(self.committed_count(),1)

    def test_duplicate_older_save_acknowledges_without_rewinding_newer_head(self):
        f=self.f;raw=f.archive();first=f.publish(raw)
        second=f.publish(f.archive(step=6),identity='2'*32,head=first.publication_id)
        before=self.state()
        self.assertEqual(f.publish(raw,identity='3'*32,head=second.publication_id),first)
        self.assertEqual(self.state(),before);self.assertEqual(self.committed_count(),2)
        self.assertEqual(self.publisher.head('alice',f.job_id),second)

    def test_new_coordinated_save_id_at_same_step_is_distinct(self):
        f=self.f;raw=f.archive();first=f.publish(raw)
        def new_save(meta):
            meta['checkpoint_id']='f'*32
            for ack in meta['rank_acknowledgments']:ack['checkpoint_id']=meta['checkpoint_id']
        second=f.publish(self.rewrite(raw,metadata=new_save),identity='2'*32,head=first.publication_id)
        self.assertNotEqual(first.publication_id,second.publication_id)
        self.assertEqual(self.committed_count(),2);self.assertEqual(self.state()[0][1],2)

    def test_missing_extra_duplicate_empty_or_corrupt_rank_member_never_commits(self):
        f=self.f;first=f.publish();before=self.state();raw=f.archive(step=6)
        bad=[self.rewrite(raw,members=lambda entries:entries.pop('rank-2.pt')),
             self.rewrite(raw,members=lambda entries:entries.update({'rank-3.pt':b'extra'})),
             self.rewrite(raw,duplicate='rank-1.pt'),
             self.rewrite(raw,members=lambda entries:entries.update({'rank-1.pt':b''})),
             self.rewrite(raw,members=lambda entries:entries.update({'rank-1.pt':b'corrupt'}))]
        for number,candidate in enumerate(bad,2):
            with self.subTest(number=number),self.assertRaises(ValueError):
                f.publish(candidate,identity=f'{number:032x}',head=first.publication_id)
        self.assertEqual(self.state(),before);self.assertEqual(self.committed_count(),1)

    def test_missing_reordered_negative_or_mixed_save_acknowledgments_are_rejected(self):
        f=self.f;first=f.publish();before=self.state();raw=f.archive(step=6)
        mutations=[lambda m:m.update(rank_acknowledgments=m['rank_acknowledgments'][:2]),
                   lambda m:m['rank_acknowledgments'].reverse(),
                   lambda m:m['rank_acknowledgments'][2].update(accepted=False),
                   lambda m:m['rank_acknowledgments'][2].update(checkpoint_id='f'*32),
                   lambda m:m['rank_acknowledgments'][2].update(state_sha256='f'*64)]
        for number,mutate in enumerate(mutations,2):
            with self.subTest(number=number),self.assertRaises(ValueError):
                f.publish(self.rewrite(raw,metadata=mutate),identity=f'{number:032x}',head=first.publication_id)
        self.assertEqual(self.state(),before)

    def test_expected_group_and_every_rank_current_fence_are_required(self):
        f=self.f;first=f.publish();before=self.state();raw=f.archive(step=6)
        changes=[lambda m:m.update(world_size=2),lambda m:m.update(group_id='f'*64),
                 lambda m:m.pop('rank_fences'),lambda m:m['rank_fences']['2'].update(generation=999),
                 lambda m:m['rank_fences']['2'].update(node_id='pi99')]
        for number,mutate in enumerate(changes,2):
            with self.subTest(number=number),self.assertRaises((ValueError,PermissionError)):
                f.publish(self.rewrite(raw,metadata=mutate),identity=f'{number:032x}',head=first.publication_id)
        self.assertEqual(self.state(),before)

    def test_new_save_cannot_regress_group_progress_or_completion(self):
        f=self.f;first=f.publish(f.archive(completed=True));before=self.state()
        for number,raw in enumerate((f.archive(step=4,completed=True),f.archive(step=6)),2):
            with self.subTest(number=number),self.assertRaises(ValueError):
                f.publish(raw,identity=f'{number:032x}',head=first.publication_id)
        self.assertEqual(self.state(),before);self.assertEqual(self.committed_count(),1)

    def test_peer_revocation_during_duplicate_stream_cannot_bypass_authority(self):
        f=self.f;raw=f.archive();first=f.publish(raw);before=self.state();owner=self
        class Revoke(io.BytesIO):
            def read(stream,size):
                with owner.db:owner.db.execute("UPDATE execution_leases SET revoked=1 WHERE node_id='pi03'")
                return super().read(size)
        with self.assertRaises(ValueError):
            self.publisher.publish('alice',f.job_id,'pi01',Revoke(raw),publication_id='2'*32,
                expected_head=first.publication_id,expected_sha256=hashlib.sha256(raw).hexdigest(),expected_size=len(raw))
        self.assertEqual(self.state(),before)

    def test_old_attempt_new_transfer_cannot_advance_latest(self):
        f=self.f;raw=f.archive();f.publish(raw);before=self.state()
        with self.db:self.db.execute('UPDATE jobs SET retry_count=1 WHERE job_id=?',(f.job_id,))
        with self.assertRaises(ValueError):f.publish(raw,identity='2'*32,head=before[0][0])
        self.assertEqual(self.state(),before)

    def race(self,conflicting):
        f=self.f;barrier=threading.Barrier(2);raw=f.archive()
        class Stream(io.BytesIO):
            waiting=True
            def read(stream,size):
                if stream.waiting:stream.waiting=False;barrier.wait(timeout=10)
                return super().read(size)
        def publish(number):
            data=(self.rewrite(raw,metadata=lambda m:m.update(created_at=2)) if conflicting and number else raw)
            with closing(f.f.connect()) as db:
                publisher=publication.LocalCheckpointPublisher(f.f.registry(db),f.root,reserve_free_bytes=0)
                publisher.initialize()
                try:
                    return publisher.publish('alice',f.job_id,'pi01',Stream(data),publication_id=f'{number+1:032x}',
                        expected_head=None,expected_sha256=hashlib.sha256(data).hexdigest(),expected_size=len(data))
                except publication.base.RegistryConflict:return None
        with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(publish,(0,1)))
        self.assertEqual(self.committed_count(),1);self.assertEqual(self.state()[0][1],1)
        self.assertEqual(self.state()[2],1)
        self.assertEqual(dict(self.db.execute('SELECT phase,COUNT(*) FROM hybrid_checkpoint_publications GROUP BY phase')),
                         {'COMMITTED':1,'REJECTED':1})
        return results

    def test_independent_concurrent_collectors_commit_same_save_once(self):
        results=self.race(False)
        self.assertIsNotNone(results[0]);self.assertEqual(results[0],results[1])

    def test_concurrent_conflicting_save_identity_has_only_one_winner(self):
        self.assertEqual(sum(result is not None for result in self.race(True)),1)

    def test_interrupted_orphan_is_not_adopted_when_same_save_is_retried(self):
        f=self.f;raw=f.archive();rename=publication._rename_noreplace
        class Crash(BaseException):pass
        def crash(*args):rename(*args);raise Crash('fixture crash before metadata commit')
        with patch.object(publication,'_rename_noreplace',side_effect=crash):
            with self.assertRaises(Crash):f.publish(raw)
        self.assertIsNone(self.publisher.head('alice',f.job_id))
        self.assertEqual(self.publisher._publication('1'*32)['phase'],'PUBLISHING')
        receipt=f.publish(raw,identity='2'*32)
        self.assertEqual(receipt.publication_id,'2'*32);self.assertEqual(self.committed_count(),1)
        self.assertEqual(self.publisher._publication('1'*32)['phase'],'PUBLISHING')
        self.assertEqual(len(list((f.root/'commits').iterdir())),2)

    def test_authenticated_existing_route_retry_returns_same_canonical_save(self):
        f=self.f;f.worker()
        collector=fixture.CheckpointCollector(self.publisher,{'pi01':f.key},get=f.get)
        first=collector.sync('alice',f.job_id,f.node,f.leases[0],publication_id='1'*32,expected_head=None)
        before=self.state()
        second=collector.sync('alice',f.job_id,f.node,f.leases[0],publication_id='2'*32,
                              expected_head=first.publication_id,skip_unchanged=False)
        self.assertEqual(first,second);self.assertEqual(self.state(),before)
        self.assertFalse(f.service.slot.locked());self.assertEqual(list(f.service.spool.iterdir()),[])


if __name__=='__main__':unittest.main(verbosity=2)
