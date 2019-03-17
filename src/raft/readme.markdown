##Raft结构建议
* 并发的goroutine中对到来事件的响应更新有两种方式:
    - 使用共享变量和锁同步
    - 中央goroutine通过channel接收消息,以此进行状态管理

* 一个Raft实例有两个时间驱动(time-driven)活动:
    - leader必须定时发送心跳
    - 除leader以外的其他成员必须在选举超时还没收到来自leader的心跳之后开始进行选举
    
* 对于选举超时的管理,最简单的方式就是在实例中维护一个变量记录最近一次收到来自leader发送的心跳的时间,然后定期检查是否超时

* 必须单独(separate)设置一个长期运行(long-running)goroutine按序将日志确认发送给applyCh.
    - 必须单独设置:发送消息到applyCh会阻塞
    - 最简单的方法是设置条件变量,每当commitIndex增加的时候,唤醒条件变量,然后发送日志确认.
* 每个RPC调用必须在其单独的一个goroutine中:
    - 对于大多数回复的收集不能因为某些不可达的实例而导致延迟
    - 对于RPC回复的处理最简单的处理方式是直接在同一个goroutine中处理 