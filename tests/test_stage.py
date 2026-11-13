"""可恢复阶段交换的协议核对。"""
import json
import tempfile
import unittest
from pathlib import Path

from warehouse.objects import ObjectStore, encode
from warehouse.typed import typed
from warehouse.stage import (STATES, StageExchange, LocalExecutor, Producer,
                             Coordinator, audit, demo)

DATA = json.loads(Path('data/source.json').read_text(encoding='utf-8'))


def reference_totals():
    merged = {}
    for row in DATA['left'] + DATA['right']:
        tkey = encode(typed(row['key'])).decode('utf-8')
        entry = merged.setdefault(tkey, {'key': row['key'], 'rows': 0, 'amount': 0})
        entry['rows'] += 1
        amount = row.get('amount')
        if isinstance(amount, int) and not isinstance(amount, bool):
            entry['amount'] += amount
    return merged


def frag_rows(prefix, seqs):
    return [{'row_id': f'{prefix}{i}', 'key': i} for i in seqs]


class StageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.store = ObjectStore(self.root / 'objects')

    def _exchange(self, **kw):
        return StageExchange(self.store, self.root / 'sx', 'q1', **kw)

    def _coordinator(self, shard_count=2, **kw):
        return Coordinator(self.store, self.root, 'q1', shard_count, **kw).start()

    def _producers(self, chunk=1, order=None):
        return [Producer('left', 0, DATA['left'], chunk=chunk, order=order),
                Producer('right', 0, DATA['right'], chunk=chunk, order=order)]

    def _drain(self, exchange):
        got = []
        while True:
            item = exchange.next_row()
            if item is None:
                return got
            got.append((item['channel'], item['seq'], item['row']['row_id']))

    # ---- 通道序号、完成标记与去重 ----

    def test_completion_order_is_free(self):
        ex = self._exchange()
        ex.complete('right', 0, 1)                       # 完成标记先到
        ex.submit('right', 0, 0, [{'row_id': 'r1', 'key': 'x'}])
        ex.submit('left', 0, 0, [{'row_id': 'l1', 'key': 'x'}])
        ex.submit('right', 0, 1, [{'row_id': 'r2', 'key': 'y'}])
        ex.complete('left', 0, 0)                        # 另一通道随后完成
        got = self._drain(ex)
        self.assertEqual([g[:2] for g in got],
                         [('left|0', 0), ('right|0', 0), ('right|0', 1)])
        self.assertEqual(ex.status()['state'], 'drained')

    def test_empty_channel_completes_immediately(self):
        ex = self._exchange()
        ex.complete('left', 0, -1)                       # 空通道完成标记为 -1
        ex.submit('right', 0, 0, [{'row_id': 'r1', 'key': 'x'}])
        ex.complete('right', 0, 0)
        self.assertEqual(len(self._drain(ex)), 1)
        self.assertEqual(ex.status()['state'], 'drained')

    def test_duplicate_transfer_not_redelivered(self):
        ex = self._exchange()
        rows = [{'row_id': 'a', 'key': 1}, {'row_id': 'b', 'key': 2}]
        first = ex.submit('left', 0, 0, rows, attempt=1)
        self.assertEqual(first['status'], 'accepted')
        again = ex.submit('left', 0, 0, rows, attempt=7)  # 重复传送
        self.assertEqual(again['status'], 'duplicate')
        self.assertEqual(ex.metrics['duplicate_frags'], 1)
        self.assertEqual(len(self._drain(ex)), 2)         # 只交付一次
        self.assertIsNone(ex.next_row())

    def test_gap_not_masked_by_completion(self):
        ex = self._exchange()
        ex.submit('left', 0, 0, frag_rows('x', (0, 1)))
        ex.submit('left', 0, 4, frag_rows('x', (4, 5)))   # 缺口 2-3
        ex.complete('left', 0, 5)                         # 完成标记不能掩盖缺口
        self.assertEqual([g[1] for g in self._drain(ex)], [0, 1])
        self.assertEqual(ex.status()['state'], 'incomplete')
        ex.submit('left', 0, 2, frag_rows('x', (2, 3)))   # 补齐缺口
        self.assertEqual([g[1] for g in self._drain(ex)], [2, 3, 4, 5])
        self.assertEqual(ex.status()['state'], 'drained')

    def test_conflicting_fragment_fails(self):
        ex = self._exchange()
        ex.submit('left', 0, 0, [{'row_id': 'a', 'key': 1}])
        with self.assertRaises(ValueError):
            ex.submit('left', 0, 0, [{'row_id': 'b', 'key': 1}])  # 同身份不同内容
        self.assertEqual(ex.status()['state'], 'failed')
        with self.assertRaises(ValueError):
            ex.next_row()

    # ---- 恢复点内容 ----

    def test_recovery_point_contents(self):
        coord = self._coordinator()
        producers = self._producers(chunk=1)
        coord.step(producers)                             # L0、R0 交付
        coord.exchange.submit('left', 0, 1, DATA['left'][1:2])
        coord.exchange.submit('right', 0, 2, DATA['right'][2:3])  # 右通道在途片段
        coord.step([])                                    # L1 交付：各通道位置不同
        coord.issue_barrier()
        coord.step([])                                    # 对齐并写恢复点
        point = coord.exchange.catalog.read()['q1|checkpoint']
        self.assertEqual(point['barrier'], 1)
        self.assertEqual(point['channels']['left|0']['frontier'], 2)
        self.assertEqual(point['channels']['right|0']['frontier'], 1)  # 非全局最大序号
        self.assertEqual(point['retained'],
                         [{'channel': 'right|0', 'first_seq': 2, 'last_seq': 2,
                           'digest': point['retained'][0]['digest'],
                           'bytes': point['retained'][0]['bytes']}])
        self.assertEqual(point['consumer']['shard_count'], 2)
        self.assertIn('0', point['consumer']['summaries'])
        summary = coord.exchange.catalog.read()['q1|barrier|1']       # 按屏障登记
        self.assertEqual(summary['summaries'], point['consumer']['summaries'])

    # ---- 重启恢复 ----

    def test_restart_recovers_without_duplicate_or_loss(self):
        coord = self._coordinator()
        producers = self._producers(chunk=1)
        coord.step(producers)                             # L0、R0 交付
        coord.issue_barrier()
        coord.step([])                                    # 屏障1对齐：检查点 frontier=1
        coord.step(producers)                             # L1、R1 交付（未检查点）
        crashed_journal = coord.journal.path              # 模拟进程重启：丢弃内存状态
        coord2 = self._coordinator()
        self.assertEqual(coord2.status()['state'], 'recoverable')
        final = coord2.run(self._producers(chunk=1))
        self.assertEqual(final['state'], 'complete')
        self.assertEqual(coord2.executor.totals(), reference_totals())
        report = audit(crashed_journal)
        self.assertEqual(report['violations'], [])
        self.assertEqual(report['missing'], [])
        self.assertEqual(report['effective_rows'], 6)
        self.assertEqual(report['generations'], [1, 2])
        rolled = report['rows']['left|0|1']               # 检查点后交付的行被重交付
        self.assertEqual([d['gen'] for d in rolled['deliveries']], [1, 2])
        self.assertEqual(rolled['effective']['gen'], 2)
        kept = report['rows']['left|0|0']                 # 检查点覆盖的行不重交付
        self.assertEqual([d['gen'] for d in kept['deliveries']], [1])
        self.assertEqual(kept['effective']['gen'], 1)

    def test_reshard_on_restart(self):
        coord = self._coordinator(shard_count=2)
        producers = self._producers(chunk=1)
        coord.step(producers)
        coord.issue_barrier()
        coord.step([])
        coord.step(producers)
        journal = coord.journal.path
        coord2 = self._coordinator(shard_count=3)         # 重启改变消费分片数量
        self.assertEqual(coord2.executor.shard_count, 3)
        self.assertEqual(coord2.executor.rows_processed, 2)  # 快照状态已重新分配
        final = coord2.run(self._producers(chunk=1))
        self.assertEqual(final['state'], 'complete')
        self.assertEqual(coord2.executor.totals(), reference_totals())
        report = audit(journal)
        self.assertEqual(report['violations'], [])
        for row in report['rows'].values():               # 有效交付落在当前分片
            eff = row['effective']
            shard = LocalExecutor('q1', 3, 2).route(
                {'key': self._key_of(row['row_id'])})
            if eff['gen'] == 2:
                self.assertEqual(eff['shard'], shard)

    def _key_of(self, row_id):
        for row in DATA['left'] + DATA['right']:
            if row['row_id'] == row_id:
                return row['key']
        raise KeyError(row_id)

    def test_stale_generation_cannot_ack_or_recycle(self):
        coord1 = self._coordinator()
        producers = self._producers(chunk=1)
        coord1.step(producers)
        coord1.issue_barrier()
        coord1.step([])
        old_exchange = coord1.exchange
        retained = old_exchange.catalog.read()['q1|checkpoint']['retained']
        coord2 = self._coordinator()                      # 第二代接管
        with self.assertRaises(ValueError):
            old_exchange.ack(1, 'left', 0, 1)             # 旧代确认被拒绝
        with self.assertRaises(ValueError):
            old_exchange.checkpoint(1, 99, {})            # 旧代检查点被拒绝
        with self.assertRaises(ValueError):
            coord1.step(producers)                        # 旧协调者被隔离
        self.assertTrue(coord1.executor.fenced)
        with self.assertRaises(ValueError):
            coord1.executor.process(0, {'row_id': 'x', 'key': 'x'})
        for ret in retained:                              # 旧代未能回收新代所需片段
            self.assertEqual(self.store.get(ret['digest']),
                             (self.store.root / ret['digest']).read_bytes())
        final = coord2.run(self._producers(chunk=1))
        self.assertEqual(final['state'], 'complete')
        report = audit(coord2.journal.path)
        self.assertGreaterEqual(report['stale_acks'], 1)
        self.assertEqual(report['violations'], [])

    # ---- 背压、溢写与回收 ----

    def test_spill_and_reload_under_memory_pressure(self):
        ex = self._exchange(memory_budget=10)             # 缓存预算装不下任何片段
        ex.submit('left', 0, 0, frag_rows('x', (0, 1)))
        ex.submit('left', 0, 2, frag_rows('x', (2,)))
        self.assertEqual(ex.metrics['spill_frags'], 2)
        self.assertGreater(ex.metrics['spill_bytes'], 0)
        self.assertEqual(ex.cache_bytes, 0)
        self.assertEqual(len(self._drain(ex)), 3)         # 交付时从磁盘重载
        self.assertEqual(ex.metrics['reload_frags'], 3)   # 首片段两行各重载一次
        self.assertGreater(ex.metrics['reload_bytes'], 0)

    def test_backpressure_bounds_pending_but_never_blocks_frontier(self):
        ex = self._exchange(pending_budget=70)
        frag = frag_rows('x', (0, 1))
        nbytes = len(encode(frag))
        self.assertLessEqual(nbytes, 70)
        ex.submit('left', 0, 0, frag)
        self._drain(ex)                                   # 消费位置前进，在途清零
        ex.submit('left', 0, 4, frag_rows('x', (4, 5)))   # 缺口前瞻片段占用在途
        self.assertFalse(ex.can_accept('left', 0, 6, nbytes))   # 超预算：背压
        self.assertTrue(ex.can_accept('left', 0, 2, nbytes))    # 覆盖消费位置总接受
        ex.submit('left', 0, 2, frag_rows('x', (2, 3)))
        self._drain(ex)
        self.assertEqual(ex.pending_bytes, 0)
        self.assertTrue(ex.can_accept('left', 0, 6, nbytes))

    def test_coordinator_throttles_without_unbounded_buffering(self):
        coord = self._coordinator(memory_budget=1 << 16, pending_budget=1)
        left = Producer('left', 0, DATA['left'], chunk=1, order=[1, 0, 2])
        right = Producer('right', 0, DATA['right'], chunk=1)
        status = coord.run([left, right])                 # 左通道先发前瞻片段被限流
        self.assertEqual(status['state'], 'incomplete')
        self.assertGreaterEqual(status['metrics']['throttled'], 1)
        self.assertLessEqual(coord.exchange.pending_bytes, 1)  # 不为最慢通道无界缓存
        coord2 = Coordinator(self.store, self.root, 'q1', 2,
                             memory_budget=1 << 16, pending_budget=1 << 16).start()
        final = coord2.run(self._producers(chunk=1))      # 恢复后放行完成
        self.assertEqual(final['state'], 'complete')
        self.assertEqual(coord2.executor.totals(), reference_totals())

    def test_recycle_reclaims_storage_after_checkpoint(self):
        coord = self._coordinator()
        final = coord.run(self._producers(chunk=1))
        self.assertEqual(final['state'], 'complete')
        metrics = final['metrics']
        self.assertEqual(metrics['recycled_frags'], 6)    # 全部片段被确认回收
        self.assertGreater(metrics['recycled_bytes'], 0)
        events = [json.loads(line) for line in
                  coord.journal.path.read_text(encoding='utf-8').splitlines()]
        submitted = [e for e in events if e['ev'] == 'submit']
        self.assertEqual(len(submitted), 6)
        for event in submitted:                           # 片段对象已实际删除
            self.assertFalse((self.store.root / event['digest']).exists())
        self.assertEqual(coord.exchange.retained_bytes, 0)
        report = audit(coord.journal.path)
        self.assertEqual(report['violations'], [])
        self.assertEqual(report['recycled_bytes'], metrics['recycled_bytes'])

    # ---- 失败与状态分类 ----

    def test_missing_retained_fragment_fails_recovery(self):
        coord = self._coordinator()
        producers = self._producers(chunk=1)
        coord.step(producers)
        coord.exchange.submit('left', 0, 2, DATA['left'][2:3])  # 在途片段
        coord.issue_barrier()
        coord.step([])                                    # 恢复点保留该片段
        point = coord.exchange.catalog.read()['q1|checkpoint']
        digest = point['retained'][0]['digest']
        (self.store.root / digest).unlink()               # 模拟保留片段丢失
        coord2 = Coordinator(self.store, self.root, 'q1', 2).start()
        status = coord2.status()
        self.assertEqual(status['state'], 'failed')
        self.assertIn('保留片段', status['failed_reason'])

    def test_status_transitions(self):
        coord = self._coordinator()
        self.assertEqual(coord.status()['state'], 'incomplete')   # 尚未完成
        producers = self._producers(chunk=1)
        coord.step(producers)
        coord.issue_barrier()
        coord.step([])
        coord2 = self._coordinator()
        self.assertEqual(coord2.status()['state'], 'recoverable')  # 可以恢复
        final = coord2.run(self._producers(chunk=1))
        self.assertEqual(final['state'], 'complete')               # 已完成
        again = self._coordinator()
        self.assertEqual(again.status()['state'], 'complete')      # 重启幂等
        self.assertEqual(set(STATES), {'incomplete', 'recoverable', 'failed', 'complete'})

    # ---- 端到端演示 ----

    def test_demo(self):
        result = demo()
        self.assertEqual(result['status']['state'], 'complete')
        self.assertEqual(result['audit']['violations'], [])
        self.assertEqual(result['audit']['missing'], [])
        self.assertEqual(result['audit']['effective_rows'], 6)
        self.assertEqual(result['audit']['generations'], [1, 2])
        self.assertGreater(result['audit']['spill_bytes'], 0)
        self.assertGreater(result['audit']['reload_bytes'], 0)
        self.assertGreater(result['audit']['recycled_bytes'], 0)
        self.assertEqual(result['totals'], reference_totals())


if __name__ == '__main__':
    unittest.main()
