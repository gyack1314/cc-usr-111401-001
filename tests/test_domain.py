"""领域资料及现有接口核对。"""
import json,tempfile,unittest
from pathlib import Path
from warehouse.objects import ObjectStore
from warehouse.exchange import inspect

class DomainTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.store=ObjectStore(self.root/'objects')
        self.data=json.loads(Path('data/source.json').read_text(encoding='utf-8'))
    def test_existing_reader(self):
        result=inspect(self.data,self.store,self.root)
        self.assertTrue(result)

    def test_replay_and_conflict(self):
        from warehouse.exchange import Exchange
        x=Exchange(self.store,self.root/'exchange');a=x.accept('r','left',0,'f',1,[{'row_id':'a','key':1}])
        b=x.accept('r','left',0,'f',2,[{'row_id':'a','key':1}]);self.assertTrue(b['replayed'])
        with self.assertRaises(ValueError):x.accept('r','left',0,'f',3,[{'row_id':'b','key':1}])
