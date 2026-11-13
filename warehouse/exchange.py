"""带逻辑身份的交换片段存取。"""
from .objects import encode
from .catalog import Catalog
import hashlib

class Exchange:
    def __init__(self, store, root):self.store=store;self.catalog=Catalog(root)
    def accept(self, run, side, partition, fragment, attempt, rows):
        if side not in ('left','right'):raise ValueError('交换侧非法')
        if len({r['row_id'] for r in rows})!=len(rows):raise ValueError('片段行身份重复')
        identity=hashlib.sha256(encode([run,side,partition,fragment])).hexdigest()
        digest=self.store.put_json(rows)
        current=self.catalog.read().get(identity)
        if current:
            if current['digest']!=digest:raise ValueError('逻辑片段内容冲突')
            return dict(current,replayed=True)
        value={'run':run,'side':side,'partition':partition,'fragment':fragment,'digest':digest}
        self.catalog.compare_update(identity,None,value)
        return dict(value,replayed=False)
    def rows(self, receipt):return self.store.json(receipt['digest'])

def inspect(data, store, root):
    service=Exchange(store,root/'exchange')
    receipts=[service.accept('read-1',side,0,'part-0',1,data[side]) for side in ('left','right')]
    return {'left_rows':len(service.rows(receipts[0])),'right_rows':len(service.rows(receipts[1])),'receipts':receipts}
