# 仓查询阶段交换的检查点与重分配恢复资料工具

Python 3.11 项目，用于读取、保存及核对协作资料仓的本地资料。不可变内容通过 SHA256 寻址，目录引用单独保存。现有领域接口位于 warehouse/exchange.py，资料契约见 docs/contracts.md，合成输入位于 data/source.json。warehouse/objects.py 负责内容校验，catalog.py 负责本地引用更新，snapshot.py 提供完整行快照基础读取。

已有读取流程使用临时目录，不改变源资料。当前只提供源码中已实现的基础操作，输出相应对象身份、行或引用摘要。领域接口的测试说明其现有边界，不承诺额外的管理或查询功能。

warehouse/stage.py 在既有交换通道之上提供可持久恢复的阶段交换：每通道连续序号与完成标记、重复传送去重、屏障与恢复点、执行代隔离的确认与回收、背压与溢写，以及可核对的调度日志。协议说明见 docs/stage-exchange.md，演示见 `python -m warehouse --stage-demo`。

运行与开发检查：

```sh
python -m unittest discover -s tests -v
python -m compileall -q warehouse tests
python -m warehouse
python -m warehouse --stage-demo
```
