# 资料与既有接口约定

交换记录中的 run、side、partition、fragment 共同标识逻辑片段，attempt 是传送尝试，payload_digest 绑定不可变行列表。同身份不同正文是冲突，旧 run 不能加入新 run。row_id 在每侧唯一，业务 key 可重复，payload 是业务列。null_equal 为 true 时两个 null 键匹配，否则不匹配。现有 exchange.py 只登记和读取片段，不执行连接；snapshot.py 提供冻结源文件的校验读取。预算单位为规范 JSON 行的字节数，另登记缓冲区和文件句柄峰值，不把操作系统缓存冒充受控内存。

所有资料均为合成，字段、身份及状态按本文解释。源码中的 inspect 入口只读取并核对现有基础接口；不会建立高级查询或仓库管理流程。
