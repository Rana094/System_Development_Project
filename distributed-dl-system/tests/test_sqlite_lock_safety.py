"""Real Linux POSIX lock preservation and multi-process WAL regression.

Temporary DB only. No controller imports, live DB, services, NFS or Pi needed.
"""
import json,os
from pathlib import Path
import sqlite3,subprocess,sys,tempfile,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from hybrid_storage import registry_schema,staging_schema,state_backup

LOCK_PROBE='''import fcntl,os,sys
fd=os.open(sys.argv[1],os.O_RDWR)
try:
 try:fcntl.lockf(fd,fcntl.LOCK_EX|fcntl.LOCK_NB,510,0x40000002,os.SEEK_SET);print('UNLOCKED')
 except BlockingIOError:print('LOCKED')
finally:os.close(fd)
'''
WORKER='''import sqlite3,sys
sys.path.insert(0,sys.argv[1])
from hybrid_storage import registry_schema as base,staging_schema as stage
path,worker=sys.argv[2],int(sys.argv[3]);db=sqlite3.connect(path,timeout=10)
db.execute('PRAGMA foreign_keys=ON');db.execute('PRAGMA synchronous=FULL');db.execute('PRAGMA wal_autocheckpoint=5')
for i in range(40):
 base.connection_ready(db)
 with db:
  db.execute('BEGIN IMMEDIATE');stage.require_local_read(db)
  db.execute('INSERT INTO probe VALUES(?,?,?)',(worker,i,b'x'*4096))
 db.execute('SELECT count(*) FROM probe').fetchone();stage.require_local_read(db)
db.close();print('40 committed')
'''
class LockTests(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory(prefix='sqlite-lock-regression-');self.addCleanup(self.t.cleanup)
  self.path=Path(self.t.name)/'fixture.sqlite'
 def connect(self):
  db=sqlite3.connect(self.path,timeout=10);db.execute('PRAGMA foreign_keys=ON');db.execute('PRAGMA synchronous=FULL')
  db.execute('PRAGMA journal_mode=WAL');db.execute('CREATE TABLE IF NOT EXISTS probe(worker INTEGER,n INTEGER,payload BLOB,PRIMARY KEY(worker,n))');db.commit()
  return db
 def lock(self):return subprocess.check_output([sys.executable,'-I','-B','-c',LOCK_PROBE,str(self.path)],text=True,timeout=10).strip()
 def check_validator(self,validator):
  db=self.connect()
  try:
   db.execute('SELECT * FROM probe').fetchall();self.assertEqual(self.lock(),'LOCKED','Test must establish a real SQLite read lock first')
   validator(db)
   self.assertEqual(self.lock(),'LOCKED','Metadata validation released SQLite POSIX locks behind its VFS')
  finally:db.close()
 def test_registry_metadata_attestation_preserves_sqlite_shared_lock(self):self.check_validator(registry_schema.connection_ready)
 def test_staging_metadata_attestation_preserves_sqlite_shared_lock(self):self.check_validator(staging_schema.require_local_read)
 def test_backup_metadata_attestation_preserves_sqlite_shared_lock(self):self.check_validator(lambda db:state_backup._attest_source(self.path))
 def test_concurrent_wal_writers_with_metadata_checks_keep_all_rows_and_integrity(self):
  db=self.connect();db.close();processes=[]
  try:
   for number in range(4):processes.append(subprocess.Popen([sys.executable,'-I','-B','-c',WORKER,str(ROOT),str(self.path),str(number)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True))
   for process in processes:
    out,err=process.communicate(timeout=60);self.assertEqual(process.returncode,0,err);self.assertEqual(out.strip(),'40 committed')
  finally:
   for process in processes:
    if process.poll() is None:process.kill();process.communicate(timeout=5)
  with sqlite3.connect(self.path) as db:
   self.assertEqual(db.execute('PRAGMA integrity_check').fetchall(),[('ok',)])
   self.assertEqual(db.execute('PRAGMA foreign_key_check').fetchall(),[])
   self.assertEqual(db.execute('SELECT count(*),sum(length(payload)) FROM probe').fetchone(),(160,160*4096))

if __name__=='__main__':unittest.main(verbosity=2)
