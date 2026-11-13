"""可持久恢复的阶段交换。

在既有 Exchange（以 run 为执行代、以片段身份寻址的本地通道）之上，为查询
运行层提供跨进程重启的上下游行流交接。这里只处理查询中间数据，不做数据源
采集。

- 每条生产通道 (run, side, partition) 拥有从 0 连续递增的序号与完成标记
  （空通道为 -1）；重复传送按逻辑身份去重，序号缺口不会被较新的完成标记
  掩盖；各通道完成到达顺序可以不同。
- 恢复点同时记录各通道消费位置、仍需保留的在途片段以及下游状态所属屏障，
  不保存单一全局最大序号。
- 屏障由协调者单调发出，下游按屏障编号登记状态摘要。
- 确认与回收按执行代隔离：旧代工作者无法确认或回收新代所需片段。
- 内存不足时片段落盘（既有对象仓）并对生产者施加背压，不为等待最慢通道
  而无界缓存。
- 调度日志记录每条逻辑行的去向，可核对中断前后的交付，并区分尚未完成、
  可以恢复与查询已失败。
"""
from pathlib import Path
import hashlib
import json
import os
import tempfile

from .objects import ObjectStore, encode
from .catalog import Catalog
from .exchange import Exchange
from .typed import typed as _typed

#: 查询状态：尚未完成 / 可以恢复 / 查询已失败 / 已完成
STATES = ('incomplete', 'recoverable', 'failed', 'complete')


def _route(tkey_text, shard_count):
    """按类型化键确定消费分片，分片数变化后仍可重算。"""
    digest = hashlib.sha256(tkey_text.encode('utf-8')).digest()
    return int.from_bytes(digest[:8], 'big') % shard_count


def _tkey(value):
    return encode(_typed(value)).decode('utf-8')


def _parse_partition(text):
    return int(text) if text.lstrip('-').isdigit() else text


class Journal:
    """追加式调度日志：每条记录落盘并同步，供中断前后核对。"""

    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            with self.path.open('rb') as stream:
                self.seq = sum(1 for _ in stream)
        else:
            self.seq = 0

    def emit(self, event):
        self.seq += 1
        record = {'jsn': self.seq, **event}
        with self.path.open('ab') as stream:
            stream.write(encode(record) + b'\n')
            stream.flush()
            os.fsync(stream.fileno())
        return record


class _Channel:
    __slots__ = ('key', 'side', 'partition', 'frontier', 'done', 'frags')

    def __init__(self, key, side, partition):
        self.key = key
        self.side = side
        self.partition = partition
        self.frontier = 0          # 下一个待交付序号（消费位置）
        self.done = None           # 完成标记：最后一个序号，空通道为 -1
        self.frags = {}            # first_seq -> {'digest','rows','bytes','last'}


class StageExchange:
    """单执行代（run）内各生产通道的有序、去重、可恢复片段交换。"""

    def __init__(self, store, root, run, memory_budget=1 << 20, pending_budget=1 << 24, log=None):
        if not run or '|' in run:
            raise ValueError('执行代身份非法')
        self.store = store
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.run = run
        self.channel_store = Exchange(store, self.root / 'exchange')
        self.catalog = Catalog(self.root / 'stage')
        self.memory_budget = int(memory_budget)
        self.pending_budget = int(pending_budget)
        self.log = log or (lambda event: None)
        self.channels = {}
        self.cache = {}
        self.cache_bytes = 0
        self.pending_bytes = 0
        self.retained_bytes = 0
        self.failed_reason = None
        self.recovered = False
        self.expected = set()
        self.metrics = {
            'spill_bytes': 0, 'spill_frags': 0,
            'reload_bytes': 0, 'reload_frags': 0,
            'recycled_bytes': 0, 'recycled_frags': 0,
            'delivered_rows': 0, 'duplicate_frags': 0,
            'consumed_frags': 0, 'checkpoints': 0,
        }
        self.generation = self.persisted_generation()

    # ---- 持久引用 ----

    def _ref(self, *parts):
        return '|'.join([self.run, *(str(p) for p in parts)])

    def persisted_generation(self):
        return self.catalog.read().get(self._ref('session'), 0)

    def begin_generation(self):
        ref = self._ref('session')
        current = self.catalog.read().get(ref)
        new = (current or 0) + 1
        self.catalog.compare_update(ref, current, new)
        self.generation = new
        return new

    def _require_generation(self, generation):
        if generation != self.persisted_generation():
            raise ValueError('执行代已过期：禁止确认、检查点或回收')

    def _channel(self, side, partition):
        if '|' in side or '|' in str(partition):
            raise ValueError('通道身份非法')
        key = f'{side}|{partition}'
        ch = self.channels.get(key)
        if ch is None:
            ch = self.channels[key] = _Channel(key, side, partition)
        return ch

    def _fail(self, reason):
        self.failed_reason = reason
        self.log({'ev': 'failed', 'reason': reason})

    def _incref(self, digest):
        ref = self._ref('ref', digest)
        count = self.catalog.read().get(ref)
        self.catalog.compare_update(ref, count, (count or 0) + 1)

    def _decref(self, digest):
        ref = self._ref('ref', digest)
        count = self.catalog.read().get(ref)
        if count is None:
            raise ValueError('引用计数缺失')
        if count == 1:
            self.catalog.remove(ref, 1)
            self.store.discard(digest)
        else:
            self.catalog.compare_update(ref, count, count - 1)

    # ---- 生产侧 ----

    def expect(self, side, partition):
        """登记计划中的生产通道；完成判定覆盖全部已登记通道，
        从未现身的通道不会被误判为完成。"""
        ch = self._channel(side, partition)
        if ch.key not in self.expected:
            self.expected.add(ch.key)
            self.catalog.compare_update(self._ref('expect', side, partition), None, True)
        return ch.key

    def submit(self, side, partition, first_seq, rows, attempt=1):
        """接收一个数据片段；重复传送去重，已消费片段直接丢弃。"""
        if self.failed_reason:
            raise ValueError(f'查询已失败：{self.failed_reason}')
        if not isinstance(first_seq, int) or isinstance(first_seq, bool) or first_seq < 0:
            raise ValueError('片段起始序号非法')
        if not rows or not all(isinstance(r, dict) and isinstance(r.get('row_id'), str) for r in rows):
            raise ValueError('片段行缺少身份')
        if len({r['row_id'] for r in rows}) != len(rows):
            raise ValueError('片段行身份重复')
        ch = self._channel(side, partition)
        last = first_seq + len(rows) - 1
        if ch.done is not None and last > ch.done:
            self._fail(f'片段超出完成标记：{ch.key}#{first_seq}')
            raise ValueError('片段超出完成标记')
        if last < ch.frontier:
            self.metrics['consumed_frags'] += 1
            self.log({'ev': 'consumed', 'channel': ch.key, 'first_seq': first_seq})
            return {'status': 'consumed', 'channel': ch.key, 'first_seq': first_seq}
        nbytes = len(encode(rows))
        try:
            receipt = self.channel_store.accept(self.run, side, partition, str(first_seq), attempt, rows)
        except ValueError:
            self._fail(f'逻辑片段内容冲突：{ch.key}#{first_seq}')
            raise
        frag_ref = self._ref('frag', side, partition, first_seq)
        known = self.catalog.read().get(frag_ref)
        if receipt['replayed'] and known is not None:
            self.metrics['duplicate_frags'] += 1
            self.log({'ev': 'duplicate', 'channel': ch.key, 'first_seq': first_seq})
            return {'status': 'duplicate', 'channel': ch.key, 'first_seq': first_seq,
                    'digest': receipt['digest']}
        meta = {'digest': receipt['digest'], 'rows': len(rows), 'bytes': nbytes, 'last': last}
        self._incref(receipt['digest'])
        if known is None:
            self.catalog.compare_update(frag_ref, None, meta)
        ch.frags[first_seq] = meta
        self.retained_bytes += nbytes
        self.pending_bytes += nbytes
        if self.cache_bytes + nbytes <= self.memory_budget:
            self.cache[(ch.key, first_seq)] = rows
            self.cache_bytes += nbytes
        else:
            self.metrics['spill_bytes'] += nbytes
            self.metrics['spill_frags'] += 1
            self.log({'ev': 'spill', 'channel': ch.key, 'first_seq': first_seq, 'bytes': nbytes})
        self.log({'ev': 'submit', 'channel': ch.key, 'first_seq': first_seq,
                  'rows': len(rows), 'bytes': nbytes, 'digest': receipt['digest']})
        return {'status': 'accepted', 'channel': ch.key, 'first_seq': first_seq,
                'digest': receipt['digest']}

    def complete(self, side, partition, last_seq):
        """登记通道完成标记；各通道完成顺序可以任意。"""
        if self.failed_reason:
            raise ValueError(f'查询已失败：{self.failed_reason}')
        if not isinstance(last_seq, int) or isinstance(last_seq, bool) or last_seq < -1:
            raise ValueError('完成序号非法')
        ch = self._channel(side, partition)
        if ch.done is not None:
            if ch.done == last_seq:
                self.log({'ev': 'complete', 'channel': ch.key, 'last_seq': last_seq, 'repeated': True})
                return {'status': 'duplicate', 'channel': ch.key}
            self._fail(f'完成标记冲突：{ch.key}')
            raise ValueError('完成标记冲突')
        if last_seq < ch.frontier - 1:
            self._fail(f'完成标记早于已交付位置：{ch.key}')
            raise ValueError('完成标记早于已交付位置')
        if any(meta['last'] > last_seq for meta in ch.frags.values()):
            self._fail(f'完成标记掩盖在途片段：{ch.key}')
            raise ValueError('完成标记掩盖在途片段')
        self.catalog.compare_update(self._ref('done', side, partition), None, last_seq)
        ch.done = last_seq
        self.log({'ev': 'complete', 'channel': ch.key, 'last_seq': last_seq})
        return {'status': 'completed', 'channel': ch.key}

    def can_accept(self, side, partition, first_seq, nbytes):
        """背压判定：覆盖消费位置的片段总是接受（避免与缺口互相等待），
        其余前瞻片段受在途预算约束，不为最慢通道无界缓存。"""
        ch = self.channels.get(f'{side}|{partition}')
        frontier = ch.frontier if ch else 0
        if first_seq <= frontier:
            return True
        return self.pending_bytes + nbytes <= self.pending_budget

    # ---- 消费侧 ----

    def next_row(self):
        """按各通道序号顺序交付下一行；缺口处停住，返回 None。"""
        if self.failed_reason:
            raise ValueError(f'查询已失败：{self.failed_reason}')
        for key in sorted(self.channels):
            ch = self.channels[key]
            hit = self._covering(ch, ch.frontier)
            if hit is None:
                continue
            first_seq, meta = hit
            rows = self._rows(ch, first_seq, meta)
            row = rows[ch.frontier - first_seq]
            ch.frontier += 1
            self.metrics['delivered_rows'] += 1
            if meta['last'] < ch.frontier:
                self.pending_bytes -= meta['bytes']
                if self.cache.pop((key, first_seq), None) is not None:
                    self.cache_bytes -= meta['bytes']
            return {'channel': key, 'seq': ch.frontier - 1, 'row': row}
        return None

    def _covering(self, ch, seq):
        for first_seq, meta in ch.frags.items():
            if first_seq <= seq <= meta['last']:
                return first_seq, meta
        return None

    def _rows(self, ch, first_seq, meta):
        cached = self.cache.get((ch.key, first_seq))
        if cached is not None:
            return cached
        rows = self.store.json(meta['digest'])
        if len(rows) != meta['rows']:
            self._fail(f'片段行数不符：{ch.key}#{first_seq}')
            raise ValueError('片段行数不符')
        self.metrics['reload_bytes'] += meta['bytes']
        self.metrics['reload_frags'] += 1
        self.log({'ev': 'reload', 'channel': ch.key, 'first_seq': first_seq, 'bytes': meta['bytes']})
        if self.cache_bytes + meta['bytes'] <= self.memory_budget:
            self.cache[(ch.key, first_seq)] = rows
            self.cache_bytes += meta['bytes']
        return rows

    # ---- 恢复点、确认与回收 ----

    def checkpoint(self, generation, barrier, consumer):
        """持久化恢复点：各通道消费位置、在途保留片段、下游状态所属屏障。"""
        self._require_generation(generation)
        channels = {}
        retained = []
        for key, ch in sorted(self.channels.items()):
            channels[key] = {'frontier': ch.frontier, 'done': ch.done}
            for first_seq, meta in sorted(ch.frags.items()):
                if meta['last'] >= ch.frontier:
                    retained.append({'channel': key, 'first_seq': first_seq,
                                     'last_seq': meta['last'], 'digest': meta['digest'],
                                     'bytes': meta['bytes']})
        point = {'format': 1, 'run': self.run, 'generation': generation, 'barrier': barrier,
                 'channels': channels, 'retained': retained, 'consumer': consumer,
                 'metrics': self.metrics_snapshot()}
        ref = self._ref('checkpoint')
        self.catalog.compare_update(ref, self.catalog.read().get(ref), point)
        self.metrics['checkpoints'] += 1
        self.log({'ev': 'checkpoint', 'gen': generation, 'checkpoint': point})
        return point

    def ack(self, generation, side, partition, upto):
        """确认通道已消费到 upto（不含），回收其下全部片段。旧执行代拒绝。"""
        if generation != self.persisted_generation():
            self.log({'ev': 'stale_ack', 'gen': generation, 'channel': f'{side}|{partition}'})
            raise ValueError('执行代已过期：禁止确认或回收')
        ch = self._channel(side, partition)
        if upto > ch.frontier:
            raise ValueError('确认超出已交付位置')
        recycled = []
        for first_seq, meta in sorted(ch.frags.items()):
            if meta['last'] < upto:
                self.channel_store.discard(self.run, side, partition, str(first_seq))
                self.catalog.remove(self._ref('frag', side, partition, first_seq), meta)
                self._decref(meta['digest'])
                del ch.frags[first_seq]
                self.retained_bytes -= meta['bytes']
                self.metrics['recycled_bytes'] += meta['bytes']
                self.metrics['recycled_frags'] += 1
                self.log({'ev': 'recycle', 'gen': generation, 'channel': ch.key,
                          'first_seq': first_seq, 'last_seq': meta['last'], 'bytes': meta['bytes']})
                recycled.append(first_seq)
        self.log({'ev': 'ack', 'gen': generation, 'channel': ch.key, 'upto': upto})
        return recycled

    def recover(self):
        """从恢复点与片段目录重建状态；保留片段缺失即判查询已失败。"""
        refs = self.catalog.read()
        self.generation = self.persisted_generation()
        point = refs.get(self._ref('checkpoint'))
        problems = []
        for name, value in refs.items():
            parts = name.split('|')
            if len(parts) < 2 or parts[0] != self.run:
                continue
            if parts[1] == 'done' and len(parts) == 4:
                self._channel(parts[2], _parse_partition(parts[3])).done = value
            elif parts[1] == 'expect' and len(parts) == 4:
                ch = self._channel(parts[2], _parse_partition(parts[3]))
                self.expected.add(ch.key)
            elif parts[1] == 'frag' and len(parts) == 5:
                ch = self._channel(parts[2], _parse_partition(parts[3]))
                ch.frags[int(parts[4])] = value
                self.retained_bytes += value['bytes']
        if point:
            for key, saved in point['channels'].items():
                side, partition = key.rsplit('|', 1)
                ch = self._channel(side, _parse_partition(partition))
                ch.frontier = saved['frontier']
                if ch.done is None:
                    ch.done = saved['done']
            for ret in point['retained']:
                ch = self.channels.get(ret['channel'])
                meta = ch.frags.get(ret['first_seq']) if ch else None
                if meta is None or meta['digest'] != ret['digest']:
                    problems.append(f"保留片段元数据缺失：{ret['channel']}#{ret['first_seq']}")
                    continue
                try:
                    self.store.get(ret['digest'])
                except (ValueError, FileNotFoundError):
                    problems.append(f"保留片段丢失或损坏：{ret['channel']}#{ret['first_seq']}")
        for key, ch in self.channels.items():
            for first_seq, meta in ch.frags.items():
                if meta['last'] >= ch.frontier:
                    try:
                        self.store.get(meta['digest'])
                    except (ValueError, FileNotFoundError):
                        problems.append(f'在途片段丢失或损坏：{key}#{first_seq}')
                    self.pending_bytes += meta['bytes']
            if ch.done is not None and ch.frontier > ch.done + 1:
                problems.append(f'消费位置超出完成标记：{key}')
        if problems:
            self._fail('；'.join(problems))
        else:
            self.recovered = True
        self.log({'ev': 'recover', 'gen': self.generation,
                  'barrier': point['barrier'] if point else None,
                  'channels': {k: {'frontier': c.frontier, 'done': c.done}
                               for k, c in sorted(self.channels.items())},
                  'problems': problems})
        return self.status()

    # ---- 状态与计量 ----

    def status(self):
        if self.failed_reason:
            state = 'failed'
        elif self.channels and all(c.done is not None and c.frontier == c.done + 1
                                   for c in self.channels.values()):
            state = 'drained'
        else:
            state = 'incomplete'
        return {'state': state, 'run': self.run, 'generation': self.generation,
                'recovered': self.recovered, 'failed_reason': self.failed_reason,
                'channels': {k: {'frontier': c.frontier, 'done': c.done, 'fragments': len(c.frags)}
                             for k, c in sorted(self.channels.items())},
                'metrics': self.metrics_snapshot()}

    def metrics_snapshot(self):
        return dict(self.metrics, cache_bytes=self.cache_bytes, cache_frags=len(self.cache),
                    pending_bytes=self.pending_bytes, retained_bytes=self.retained_bytes)


class LocalExecutor:
    """下游本地执行替身：按键哈希分片，折叠查询中间行，可按屏障快照。"""

    def __init__(self, run, shard_count, generation):
        if shard_count < 1:
            raise ValueError('消费分片数量非法')
        self.run = run
        self.shard_count = shard_count
        self.generation = generation
        self.shards = [{} for _ in range(shard_count)]
        self.rows_processed = 0
        self.fenced = False

    def route(self, row):
        return _route(_tkey(row.get('key')), self.shard_count)

    def process(self, shard, row):
        if self.fenced:
            raise ValueError('执行代已过期：工作者已被隔离')
        tkey = _tkey(row.get('key'))
        entry = self.shards[shard].setdefault(tkey, {'key': row.get('key'), 'rows': 0, 'amount': 0})
        entry['rows'] += 1
        amount = row.get('amount')
        if isinstance(amount, int) and not isinstance(amount, bool):
            entry['amount'] += amount
        self.rows_processed += 1

    def snapshot(self, store):
        merged = self.totals()
        digest = store.put_json(merged)
        summaries = {str(i): {'rows': sum(e['rows'] for e in shard.values()), 'keys': len(shard)}
                     for i, shard in enumerate(self.shards)}
        return {'shard_count': self.shard_count, 'state': digest, 'summaries': summaries}

    @classmethod
    def restore(cls, run, generation, shard_count, store, snapshot):
        """按新分片数重新分配已快照的下游状态。"""
        executor = cls(run, shard_count, generation)
        merged = store.json(snapshot['state'])
        for tkey, entry in merged.items():
            executor.shards[_route(tkey, shard_count)][tkey] = dict(entry)
        executor.rows_processed = sum(e['rows'] for e in merged.values())
        return executor

    def fence(self):
        self.fenced = True

    def totals(self):
        merged = {}
        for shard in self.shards:
            merged.update(shard)
        return merged


class Producer:
    """上游阶段替身：把通道行流切成带连续序号的片段，最后给出完成标记。"""

    def __init__(self, side, partition, rows, chunk=2, order=None):
        if chunk < 1:
            raise ValueError('片段粒度非法')
        self.side = side
        self.partition = partition
        fragments = [(first, rows[first:first + chunk]) for first in range(0, len(rows), chunk)]
        if order is not None:
            fragments = [fragments[i] for i in order]
        self.actions = [{'kind': 'frag', 'side': side, 'partition': partition,
                         'first_seq': first, 'rows': part} for first, part in fragments]
        self.actions.append({'kind': 'done', 'side': side, 'partition': partition,
                             'last_seq': len(rows) - 1})
        self.index = 0

    def peek(self):
        return self.actions[self.index] if self.index < len(self.actions) else None

    def pop(self):
        self.index += 1

    @property
    def remaining(self):
        return len(self.actions) - self.index


class Coordinator:
    """控制面：屏障、确认、重新分配与调用者显式驱动的调度（不依赖墙钟）。"""

    def __init__(self, store, root, run, shard_count, memory_budget=1 << 20, pending_budget=1 << 24):
        self.store = store
        self.root = Path(root)
        self.run_id = run
        self.journal = Journal(self.root / 'journal.jsonl')
        self.exchange = StageExchange(store, self.root, run,
                                      memory_budget=memory_budget,
                                      pending_budget=pending_budget,
                                      log=self.journal.emit)
        self.shard_count = shard_count
        self.generation = None
        self.executor = None
        self.last_barrier = 0
        self.pending_barriers = []
        self.throttled = 0
        self.recovered = False

    def start(self):
        """启动新执行代；存在持久状态时从恢复点继续，分片数可以不同。"""
        refs = self.exchange.catalog.read()
        point = refs.get(self.exchange._ref('checkpoint'))
        has_state = point is not None or any(
            name.split('|')[1:2] in (['frag'], ['done']) for name in refs
            if name.startswith(f'{self.run_id}|'))
        self.generation = self.exchange.begin_generation()
        if has_state:
            self.exchange.recover()
            self.recovered = True
            if point is not None:
                self.last_barrier = point['barrier']
                self.executor = LocalExecutor.restore(self.run_id, self.generation,
                                                      self.shard_count, self.store,
                                                      point['consumer'])
            else:
                self.executor = LocalExecutor(self.run_id, self.shard_count, self.generation)
        else:
            self.executor = LocalExecutor(self.run_id, self.shard_count, self.generation)
        self.journal.emit({'ev': 'generation', 'generation': self.generation,
                           'shard_count': self.shard_count, 'recovered': self.recovered})
        return self

    def _check_generation(self):
        if self.generation != self.exchange.persisted_generation():
            if self.executor is not None:
                self.executor.fence()
            raise ValueError('执行代已过期：协调者已被隔离')

    def issue_barrier(self):
        """单调发出屏障，记录各通道目标位置；对齐后持久化恢复点并确认回收。"""
        self._check_generation()
        self.last_barrier += 1
        cuts = {k: c.frontier for k, c in self.exchange.channels.items()}
        self.pending_barriers.append({'barrier': self.last_barrier, 'cuts': cuts})
        self.journal.emit({'ev': 'barrier', 'gen': self.generation,
                           'barrier': self.last_barrier, 'cuts': cuts})
        return self.last_barrier

    def _align(self):
        for pending in list(self.pending_barriers):
            aligned = all(self.exchange.channels[k].frontier >= cut
                          for k, cut in pending['cuts'].items())
            if not aligned:
                continue
            consumer = self.executor.snapshot(self.store)
            self.exchange.catalog.compare_update(
                self.exchange._ref('barrier', pending['barrier']), None,
                {'generation': self.generation, 'summaries': consumer['summaries'],
                 'state': consumer['state']})
            point = self.exchange.checkpoint(self.generation, pending['barrier'], consumer)
            for key, saved in point['channels'].items():
                ch = self.exchange.channels[key]
                self.exchange.ack(self.generation, ch.side, ch.partition, saved['frontier'])
            self.pending_barriers.remove(pending)

    def step(self, producers):
        """推进一轮：拉取上游（受背压约束）、交付可交付行、对齐屏障。"""
        self._check_generation()
        progress = False
        for producer in producers:
            self.exchange.expect(producer.side, producer.partition)
            action = producer.peek()
            if action is None:
                continue
            if action['kind'] == 'frag':
                nbytes = len(encode(action['rows']))
                if self.exchange.can_accept(action['side'], action['partition'],
                                            action['first_seq'], nbytes):
                    self.exchange.submit(action['side'], action['partition'],
                                         action['first_seq'], action['rows'])
                    producer.pop()
                    progress = True
                else:
                    self.throttled += 1
                    self.journal.emit({'ev': 'throttle',
                                       'channel': f"{action['side']}|{action['partition']}",
                                       'first_seq': action['first_seq'],
                                       'pending_bytes': self.exchange.pending_bytes})
            else:
                self.exchange.complete(action['side'], action['partition'], action['last_seq'])
                producer.pop()
                progress = True
        while True:
            item = self.exchange.next_row()
            if item is None:
                break
            progress = True
            shard = self.executor.route(item['row'])
            self.journal.emit({'ev': 'deliver', 'gen': self.generation,
                               'channel': item['channel'], 'seq': item['seq'],
                               'row_id': item['row']['row_id'], 'shard': shard})
            self.executor.process(shard, item['row'])
        self._align()
        self._finalize()
        return progress

    def _finalize(self):
        if self.exchange.status()['state'] != 'drained' or self.pending_barriers:
            return
        if not self._covered():
            self.issue_barrier()
            self._align()

    def _covered(self):
        point = self.exchange.catalog.read().get(self.exchange._ref('checkpoint'))
        return point is not None and all(
            point['channels'].get(k, {}).get('frontier') == c.frontier
            for k, c in self.exchange.channels.items())

    def run(self, producers, max_steps=1000):
        steps = 0
        while steps < max_steps and self.status()['state'] not in ('complete', 'failed'):
            if not self.step(producers):
                break
            steps += 1
        return self.status()

    def status(self):
        est = self.exchange.status()
        if est['state'] == 'failed':
            state = 'failed'
        elif est['state'] == 'drained':
            state = 'complete' if self._covered() else (
                'recoverable' if self.recovered else 'incomplete')
        elif self.recovered:
            state = 'recoverable'
        else:
            state = 'incomplete'
        return {'state': state, 'run': self.run_id, 'generation': self.generation,
                'barrier': self.last_barrier, 'recovered': self.recovered,
                'failed_reason': est['failed_reason'], 'channels': est['channels'],
                'metrics': dict(est['metrics'], throttled=self.throttled)}


def audit(path):
    """按调度日志核对每条逻辑行的去向与协议守恒（中断前后可对照）。"""
    events = [json.loads(line) for line in Path(path).read_text(encoding='utf-8').splitlines()
              if line.strip()]
    deliveries = {}
    checkpoints = []
    recycles = []
    completes = {}
    violations = []
    restored = {}
    latest_frontiers = {}
    gens = set()
    spill_bytes = reload_bytes = recycled_bytes = 0
    throttled = stale_acks = 0
    for index, event in enumerate(events):
        kind = event['ev']
        if kind == 'generation':
            restored[event['generation']] = dict(latest_frontiers)
            gens.add(event['generation'])
        elif kind == 'deliver':
            key = (event['channel'], event['seq'])
            deliveries.setdefault(key, []).append(
                {'gen': event['gen'], 'shard': event['shard'], 'row_id': event['row_id']})
            gens.add(event['gen'])
            if event['seq'] < restored.get(event['gen'], {}).get(event['channel'], 0):
                violations.append(f"交付低于恢复位置：{event['channel']}#{event['seq']}"
                                  f"（代{event['gen']}）")
        elif kind == 'checkpoint':
            point = event['checkpoint']
            frontiers = {k: c['frontier'] for k, c in point['channels'].items()}
            checkpoints.append({'index': index, 'gen': point['generation'],
                                'barrier': point['barrier'], 'frontiers': frontiers,
                                'retained': {(r['channel'], r['first_seq'])
                                             for r in point['retained']}})
            latest_frontiers = frontiers
            gens.add(point['generation'])
        elif kind == 'recycle':
            recycles.append({'index': index, **event})
            recycled_bytes += event['bytes']
        elif kind == 'spill':
            spill_bytes += event['bytes']
        elif kind == 'reload':
            reload_bytes += event['bytes']
        elif kind == 'throttle':
            throttled += 1
        elif kind == 'stale_ack':
            stale_acks += 1
        elif kind == 'complete' and not event.get('repeated'):
            if event['channel'] in completes and completes[event['channel']] != event['last_seq']:
                violations.append(f"完成标记冲突：{event['channel']}")
            completes[event['channel']] = event['last_seq']
    for (channel, seq), records in deliveries.items():
        seen = set()
        for record in records:
            if record['gen'] in seen:
                violations.append(f'同代重复交付：{channel}#{seq}（代{record["gen"]}）')
            seen.add(record['gen'])
    covered = {}
    for point in checkpoints:
        gen_cover = covered.setdefault(point['gen'], {})
        for channel, frontier in point['frontiers'].items():
            gen_cover[channel] = max(gen_cover.get(channel, 0), frontier)
    last_gen = max(gens) if gens else None
    effective = {}
    for (channel, seq), records in sorted(deliveries.items()):
        for record in records:
            if record['gen'] == last_gen or seq < covered.get(record['gen'], {}).get(channel, 0):
                if (channel, seq) in effective:
                    violations.append(f'有效交付重复：{channel}#{seq}')
                effective[(channel, seq)] = record
    for recycle in recycles:
        covering = [p for p in checkpoints
                    if p['index'] < recycle['index'] and p['gen'] == recycle['gen']
                    and p['frontiers'].get(recycle['channel'], 0) > recycle['last_seq']]
        if not covering:
            violations.append(f"回收缺少覆盖检查点：{recycle['channel']}#{recycle['first_seq']}")
        elif (recycle['channel'], recycle['first_seq']) in covering[-1]['retained']:
            violations.append(f"回收了检查点保留片段：{recycle['channel']}#{recycle['first_seq']}")
    missing = [f'{channel}#{seq}' for channel, last in completes.items()
               for seq in range(last + 1) if (channel, seq) not in effective]
    rows = {f'{channel}|{seq}': {'row_id': records[0]['row_id'], 'deliveries': records,
                                 'effective': effective.get((channel, seq))}
            for (channel, seq), records in sorted(deliveries.items())}
    return {'rows': rows, 'violations': violations, 'missing': missing,
            'effective_rows': len(effective),
            'expected_rows': sum(last + 1 for last in completes.values()),
            'generations': sorted(gens), 'checkpoints': len(checkpoints),
            'spill_bytes': spill_bytes, 'reload_bytes': reload_bytes,
            'recycled_bytes': recycled_bytes, 'throttled': throttled,
            'stale_acks': stale_acks}


def demo(source='data/source.json'):
    """脚本化演示：中断、改变分片数恢复、核对去向并留下计量。"""
    data = json.loads(Path(source).read_text(encoding='utf-8'))
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        store = ObjectStore(root / 'objects')
        first = Coordinator(store, root, 'demo', shard_count=2,
                            memory_budget=64, pending_budget=1 << 16).start()
        producers = [Producer('left', 0, data['left'], chunk=1),
                     Producer('right', 0, data['right'], chunk=1)]
        first.step(producers)
        first.issue_barrier()
        first.step(producers)
        # 模拟下游进程重启：改变消费分片数，从恢复点继续；上游重发被去重
        second = Coordinator(store, root, 'demo', shard_count=3,
                             memory_budget=64, pending_budget=1 << 16).start()
        resumed = [Producer('left', 0, data['left'], chunk=1),
                   Producer('right', 0, data['right'], chunk=1)]
        final = second.run(resumed)
        report = audit(root / 'journal.jsonl')
        return {'status': final, 'totals': second.executor.totals(),
                'audit': {k: report[k] for k in ('violations', 'missing', 'effective_rows',
                                                 'expected_rows', 'generations', 'checkpoints',
                                                 'spill_bytes', 'reload_bytes',
                                                 'recycled_bytes', 'throttled', 'stale_acks')}}
