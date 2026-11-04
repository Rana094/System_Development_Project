"""Deployment safety and configuration checks without SSH or the live cluster."""
import importlib.util
import io
import json
import os
from pathlib import Path
import sqlite3
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'tools'))
import cluster_setup as setup


class DeploymentTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.node = {'id':'lab01','ip':'192.168.10.201','port':5000}

    def test_invalid_inventory_is_rejected(self):
        for field, value in [('id','-oProxyCommand=bad'),('id','local'),('ip','127.0.0.1'),
                             ('ip','0.0.0.0'),('port',True),('port',65536),
                             ('worker_root','../elsewhere'),('worker_root','/'),
                             ('worker_root','/tmp/a\nExecStart=bad'),('worker_root','/tmp/$HOME')]:
            with self.subTest(field=field,value=value),self.assertRaises(ValueError):
                setup.validate_node(dict(self.node,**{field:value}))

    def test_atomic_registration_preserves_other_settings_and_rejects_duplicates(self):
        path = self.root/'nodes.json'
        path.write_text(json.dumps({'note':'keep','nodes':[dict(self.node,worker_python='custom/bin/python')]}))
        setup.register(dict(self.node,ip='192.168.10.202'),path)
        doc = json.loads(path.read_text())
        self.assertEqual(doc['note'],'keep')
        self.assertEqual(doc['nodes'][0]['worker_python'],'custom/bin/python')
        setup.register({'id':'lab02','ip':'192.168.10.203','port':5000},path)
        self.assertEqual(len(setup.load_nodes(path)),2)
        before = path.read_bytes()
        with self.assertRaises(ValueError):
            setup.register(dict(self.node,id='lab03',ip='192.168.10.203'),path)
        self.assertEqual(path.read_bytes(),before)

    def test_failed_atomic_write_keeps_inventory(self):
        path = self.root/'nodes.json'
        path.write_text('{"nodes": []}')
        with patch.object(setup.os,'replace',side_effect=OSError('disk error')):
            with self.assertRaises(OSError): setup.register(self.node,path)
        self.assertEqual(json.loads(path.read_text()),{'nodes':[]})

    def test_bundle_excludes_runtime_and_controller_secrets(self):
        data, hashes = setup.bundle()
        with tarfile.open(fileobj=io.BytesIO(data),mode='r:gz') as archive:
            names = archive.getnames()
        self.assertEqual(set(names),set(hashes))
        self.assertIn('agent/lease_runner.py',names)
        self.assertIn('agent/dl_checkpoint.py',names)
        self.assertFalse(any(name.startswith(('storage/','controller/','logs/','.venv/')) for name in names))

    def test_maintenance_excludes_dispatcher_and_portal(self):
        for relative in ['controller/.dispatcher.lock','storage/portal/.server.lock']:
            path = self.root/relative
            path.parent.mkdir(parents=True,exist_ok=True)
            with path.open('a') as lock:
                setup.fcntl.flock(lock,setup.fcntl.LOCK_EX|setup.fcntl.LOCK_NB)
                with self.assertRaisesRegex(ValueError,'Stop the dispatcher'):
                    with setup.maintenance(self.root): pass
        with setup.maintenance(self.root): pass

    def test_maintenance_refuses_active_or_reserved_jobs(self):
        (self.root/'controller').mkdir()
        with sqlite3.connect(self.root/'controller/jobs.db') as db:
            db.execute('CREATE TABLE jobs(job_id TEXT,status TEXT)')
            db.execute('CREATE TABLE resource_reservations(node_id TEXT)')
            db.execute("INSERT INTO jobs VALUES('job-1','RUNNING')")
        with self.assertRaisesRegex(ValueError,'Active jobs'):
            with setup.maintenance(self.root): pass
        with sqlite3.connect(self.root/'controller/jobs.db') as db:
            db.execute("UPDATE jobs SET status='COMPLETED'")
            db.execute("INSERT INTO resource_reservations VALUES('lab01')")
        with self.assertRaises(ValueError): setup.assert_no_active_jobs(self.root)

    def test_service_paths_with_spaces_and_percent_are_quoted(self):
        units = setup.controller_units(Path('/home/user/System Project/100%complete'))
        self.assertIn('WorkingDirectory=/home/user/System Project/100%%complete',units['dl-dispatcher.service'])
        self.assertIn('"--production"',units['dl-student-portal.service'])
        self.assertIn('KillMode=control-group',units['dl-dispatcher.service'])

    def test_configurable_worker_identity_and_interface(self):
        spec = importlib.util.spec_from_file_location('deployment_test_agent',ROOT/'agent/agent.py')
        agent = importlib.util.module_from_spec(spec)
        with patch.dict(os.environ,{'DL_NODE_ID':'lab01','DL_NODE_ADDRESS':'192.168.10.201','DL_NETWORK_INTERFACE':'eno1'}):
            spec.loader.exec_module(agent)
            self.assertEqual(agent.node_id(),'lab01')
            self.assertEqual(agent.INTERFACE,'eno1')
            self.assertEqual(agent.get_ip_address(),'192.168.10.201')
            with agent.app.test_client() as client:
                health = client.get('/health').get_json()
            self.assertEqual(health['node'],'lab01')
            self.assertIn('architecture',health)
        with patch.dict(os.environ,{},clear=True),patch.object(agent.socket,'gethostname',return_value='existing-pi'):
            self.assertEqual(agent.node_id(),'existing-pi')


if __name__ == '__main__': unittest.main()
