"""Offline hybrid source packaging; no controller import, SSH or deployment."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from hybrid_storage import deployment_review as review


class ReviewTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)

    def test_explicit_two_worker_manifest_keeps_pi01_deferred_and_never_apply_ready(self):
        with patch('socket.socket', side_effect=AssertionError('offline only')):
            plan = review.review(['pi02', 'pi03'])
        self.assertEqual([n['id'] for n in plan['selected_workers']], ['pi02', 'pi03'])
        self.assertEqual(plan['deferred_workers'], ['pi01'])
        self.assertFalse(plan['all_three_workers_selected'])
        self.assertFalse(plan['all_three_pi_acceptance_proven']); self.assertFalse(plan['apply_ready'])
        for role in ('worker', 'controller'):
            for name in review.REQUIRED[role]: self.assertIn(name, plan['files'][role])
        self.assertNotIn('config/storage.json', plan['files']['worker'])
        self.assertNotIn('config/nodes.json', plan['files']['worker'])
        for nodes in ([], ['pi02', 'pi02'], ['pi99'], ['../pi02']):
            with self.subTest(nodes=nodes), self.assertRaises(ValueError): review.review(nodes)

    def test_archives_are_reproducible_regular_source_and_hashes_match_each_member(self):
        one = review.build(['pi02', 'pi03'], self.base/'one')
        two = review.build(['pi02', 'pi03'], self.base/'two')
        self.assertEqual(one, two)
        for role, entry in one['archives'].items():
            archive_path = self.base/'one'/entry['filename']
            self.assertEqual(hashlib.sha256(archive_path.read_bytes()).hexdigest(), entry['sha256'])
            self.assertEqual(archive_path.stat().st_mode & 0o777, 0o600)
            with tarfile.open(archive_path) as archive:
                self.assertEqual(set(archive.getnames()), set(one['files'][role]))
                for member in archive:
                    self.assertTrue(member.isfile()); self.assertEqual(member.mode, 0o644)
                    data = archive.extractfile(member).read()
                    self.assertEqual(dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest()),
                                     one['files'][role][member.name])
        self.assertEqual(json.loads((self.base/'one/review.json').read_text()), one)

    def test_existing_output_and_symlinked_ancestors_are_preserved(self):
        target = self.base/'existing'; target.mkdir(); keep = target/'keep'; keep.write_text('keep')
        with self.assertRaises(FileExistsError): review.build(['pi02', 'pi03'], target)
        self.assertEqual(keep.read_text(), 'keep')
        alias = self.base/'alias'; alias.symlink_to(target, target_is_directory=True)
        with self.assertRaises(OSError): review.build(['pi02', 'pi03'], alias/'new')
        self.assertFalse((target/'new').exists())

    def fixture(self):
        root = self.base/'source'; root.mkdir()
        # Build only required source entries, never a runtime-data copy.
        names = set(name for required in review.REQUIRED.values() for name in required)
        names.update(('config/nodes.json', 'config/runtime-requirements.txt'))
        names.update('config/storage-examples/'+name for name in review.EXAMPLES)
        for name in names:
            target = root/name; target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes((ROOT/name).read_bytes())
        return root

    def test_unknown_runtime_files_excluded_and_unsafe_source_rejected(self):
        root = self.fixture()
        for name in ('controller/jobs.db', 'agent/credentials.json', 'storage/input.bin',
                     'config/storage.json', 'hybrid_storage/.private-key', 'agent/__pycache__/runtime.pyc'):
            path = root/name; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(b'private')
        document = review.review(['pi02', 'pi03'], root=root)
        names = set().union(*(set(files) for files in document['files'].values()))
        self.assertFalse(any(name.endswith(('.db', '.pyc')) or 'credentials' in name for name in names))
        target = root/'agent/staging.py'; target.unlink(); target.symlink_to(ROOT/'agent/staging.py')
        with self.assertRaises(OSError): review.review(['pi02', 'pi03'], root=root)

    def test_changed_source_aborts_without_completion_marker_or_overwriting_other_files(self):
        original = review._read; count = 0
        def changed(root, name):
            nonlocal count
            data = original(root, name)
            if name == 'agent/staging.py':
                count += 1
                if count > 2: return data+b'\n# changed after catalog\n'
            return data
        with patch.object(review, '_read', side_effect=changed), self.assertRaisesRegex(ValueError, 'Source changed'):
            review.build(['pi02', 'pi03'], self.base/'interrupted')
        self.assertFalse((self.base/'interrupted/review.json').exists())

    def test_preview_CLI_never_imports_controller_or_sqlite(self):
        code = """import sys
from hybrid_storage.deployment_review import review
review(['pi02','pi03'])
assert 'job_store' not in sys.modules and 'sqlite3' not in sys.modules
"""
        result = subprocess.run([sys.executable, '-B', '-c', code], cwd=ROOT,
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == '__main__': unittest.main(verbosity=2)
