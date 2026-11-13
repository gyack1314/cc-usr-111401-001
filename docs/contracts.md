# 资料与既有接口约定

交换基础接口的 run、side、partition、fragment 共同标识逻辑片段，attempt 只是传送尝试，digest绑定不可变行列表；side保留left/right名称，仅代表两个生产输入，绝不隐含连接语义。row_id在一个生产通道内唯一，业务值可以重复。data/source.json包含两条合成输入流。新增协议中的每通道序号从0连续递增，完成消息给出最后一个序号，空通道为-1；屏障编号由协调者单调发出，并且下游以屏障编号登记其状态摘要。调度接口由调用者显式驱动，不依赖墙钟sleep。既有Exchange只保存及校验片段；消费位置、屏障、背压与回收由 warehouse/stage.py 按上述约定实现，协议细节见 docs/stage-exchange.md。

资料均为合成，现有inspect读取并核对基础接口。
