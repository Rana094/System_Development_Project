"""Private mount argument contracts only; actual kernel probes run separately."""
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from hybrid_storage.linux_namespace import command

class NamespaceTests(unittest.TestCase):
    def test_only_mount_operands_replaced_and_student_args_untouched(self):
        mount=Mock();original=['/usr/bin/bwrap','--ro-bind-fd','3','/','--bind-fd','4','/results','--','/usr/bin/python3','--ro-bind-fd','not-a-mount']
        result=command(original,[3,4],mount=mount)
        self.assertEqual(result,['/usr/bin/bwrap','--ro-bind','/tmp/dl-binding-1','/','--bind','/tmp/dl-binding-2','/results','--','/usr/bin/python3','--ro-bind-fd','not-a-mount'])
        mount.assert_any_call(3,Path('/tmp/dl-binding-1'));mount.assert_any_call(4,Path('/tmp/dl-binding-2'))
    def test_unknown_duplicate_missing_and_relative_descriptors_rejected(self):
        for argv,fds in [(['/bin/sh','--'],[]),(['/usr/bin/bwrap','--ro-bind-fd','5','/','--','true'],[3]),
             (['/usr/bin/bwrap','--bind-fd','3','relative','--','true'],[3]),
             (['/usr/bin/bwrap','--bind-fd','3','/','--bind-fd','3','/x','--','true'],[3]),
             (['/usr/bin/bwrap','--','true'],[3]),(['/usr/bin/bwrap'],[]),(['/usr/bin/bwrap','--','true'],[3,3])]:
            with self.subTest(argv=argv),self.assertRaises(ValueError):command(argv,fds,mount=Mock())

if __name__=='__main__':unittest.main(verbosity=2)
