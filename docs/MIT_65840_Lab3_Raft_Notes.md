# MIT 6.5840 Lab 3: Raft 实现详细笔记

这篇笔记详细整理了 MIT 6.5840 (原 6.824) Lab 3 Raft 算法实现的方方面面. Raft 的核心是将复杂的共识性问题拆分成几个相对独立的子问题: Leader Election(领导者选举)、Log Replication(日志复制)、Persistence(持久化)以及 Log Compaction(日志压缩/快照). 

---

## 整体架构与核心法则

在真正进入代码前, 需要牢记 Raft 的几条核心法则(很多坑都是因为没有严格遵守论文 Figure 2 导致的): 
1. **Term 的至高无上性**: 任何 RPC 的请求或响应中, 只要带有比当前 server 更高的 `Term`, 该 server 必须**立刻**转为 Follower 并更新自己的 `Term`, 同时重置投票 `VotedFor = -1`. 这一点对于 Leader 和 Follower 发送或者接收 RPC 调用的时候均一致. 
2. **锁的严格保护**: Server 所有的内部状态读写必须加锁, 但**切忌在持有锁时进行网络 RPC 调用**. 
3. **状态检查机制**: 由于 RPC 途中不持有锁, 经过漫长的网络延迟获取到 reply 重新加锁后, **当前 server 的状态可能已经发生巨变**. 必须重新检查身板: `if rf.CurrentTerm != args.Term || rf.State != StateLeader`. 

---

## Part 3A: Leader Election (领导者选举)

### 思路与具体内容
Raft 采用类似于心跳(Heartbeat)的机制来维持 Leader 地位. 如果 Follower 在一段时间内(Election Timeout)没有收到合法 Leader 的心跳或 AppendEntries, 就会认为系统中没有可用的 Leader, 从而发起选举. 

### 实现步骤
1. **Ticker Goroutine (`ticker`)**: 后台死循环, 周期性检查距上一次收到心跳的时间. 如果超时, 则增加自身的 Term, 转为 Candidate, 投票给自己, 并向其他节点并行发送 `RequestVote` RPC. 
2. **随机超时时间**: 为了防止选票被持久化平分(Split Vote), 节点的 Election Timeout 必须加上一定的随机抖动. 在实现中采用了 `300ms + rand(200ms)`. 
3. **RequestVote RPC**:
   - 如果发送者的 Term 比自己小, 果断拒绝. 
   - 如果当前处于 `VotedFor == -1` 或者已经投给该 Candidate, 且 Candidate 的日志**至少和自己一样新**(判断条件: 最后一条日志的 Term 更大, 或者 Term 相同但 Index 更大), 则投票给该 Candidate. 

### 细节与坑点
- **坑点 1: 重置 Election Timer 的时机**: 只有在以下三种情况应该重置(更新 `rf.LastHeartbeat`): 
  - 收到来自当前合法 Leader 的 `AppendEntries` 等 RPC (判断 Term 大小符合要求). 
  - 给其他 Candidate 投出了赞成票(`VoteGranted == true`). 
  - 自己主动发起了一次选举(变成 Candidate). 
  如果收到旧 Leader 或者没投票的情况错误重置, 会导致整个系统选举缓慢甚至无法选出 Leader. 
- **坑点 2: 并发计票**: 向所有 Peer 发送 RPC 需要起多个 Goroutine 并发执行, 收集返回结果. 在判断自己是否成功当选为 Leader 时, 使用 `atomic.AddInt32` 或在互斥锁保护下统计 `voteCount`. 此时一定要检查自身是否还是 `StateCandidate` 和原来的 `Term`. 

---

## Part 3B: Log Replication (日志复制与快速恢复)

### 思路与具体内容
一旦 Leader 选出, 客户端将请求发送给 Leader, Leader 将请求追加到本地日志, 并并行发起 `AppendEntries` RPC 将这条日志复制到其他机器. 当日志被集群中的多数派确认写入后, Leader 将其标记为 Committed, 并在应用层的状态机执行(Apply). 

### 实现步骤
1. **状态机通知 (`Start` 接口)**: Leader 接收 Command 并追加日志. 
2. **广播请求 (`sendAppendEntries`)**: Leader周期性的根据每个记录的 `NextIndex`, 发送从 `PrevLogIndex` 往后的所有日志. 如果 `NextIndex` 对应的是心跳, 则 Entries 为空. 
3. **日志一致性检查 (Follower 侧)**:
   - 比较 `rf.Log[PrevLogIndex]` 的 Term 是否等于传入的 `PrevLogTerm`. 如果不符合, 则存在冲突, 拒绝附加. 
   - 寻找日志冲突点进行截断, 将新传来的所有 Entry 追加. 
4. **CommitIndex 更新**:
   - **Leader**: 收到多数派 `reply.Success == true` 后, 将该节点的 `MatchIndex` 推进. 然后Leader 从后往前遍历每一条属于**当前 Term**的日志, 如果满足这个 index 在半数以上节点都复制成功(`matchCount > peers/2`), 则更新 Leader 的 `CommitIndex`. 
   - **Follower**: 若 RPC 附加成功, 并且 `LeaderCommit > CommitIndex`, 则设定 `CommitIndex = min(LeaderCommit, 最后一条新日志的 index)`. 
5. **Applier Goroutine (`applier`)**: 当 `CommitIndex > LastApplied` 时, 将两者之间的 Command 转化为 `ApplyMsg` 投入 `ApplyCh` 以供状态机执行. 一定要注意在投入 Channel 时不要拿着锁, 否则会造成死锁! 

### 细节与优化(快速恢复 Fast Rollback)
如果在网络严重分区或者发生频繁宕机重启的情况下, Follower 与 Leader 的日志可能相差成百上千条. 如果 Leader 被拒绝后每次将 `NextIndex` 减一重试, 会导致 RPC 风暴并且同步非常慢. 必须实现 Fast Rollback 优化: 
- **Follower 侧**: 
  - 如果日志不足(连 `PrevLogIndex` 都达不到), 回复 `Xterm = -1, LogLastIndex = 这里最后的长度`. 
  - 如果是有相同 Index 但 Term 不一致发生冲突, 回复冲突处日志的 `Xterm`, 并在自己的日志中找到这个 `Xterm` 的最早一条出现的 Index 赋值给 `LogFirstIndex`. 
- **Leader 侧**根据回传快速后退: 
  - 如果 `Xterm == -1`, `NextIndex` 降至 `LogLastIndex`. 
  - 如果 Leader 本地能找到该 `Xterm`, 令 `NextIndex` = Leader本地该Term最后一条日志的Index + 1. 
  - 否则令 `NextIndex = LogFirstIndex`. 这样下次发送给这个 Follower 的 entries 就可以会直接跳过整个无法匹配的 `Xterm`. 

### 坑点
- **坑点 3: 只能提交自身 Term 的日志**: Raft 的核心定限——Leader 绝不允许通过统计多数派来主动 Commit 先前 Term 的日志, 只能 Commit 自己当前 Term 的日志, 之前的日志会作为附带品一起提交! 这在 `Leader` 的 `CommitIndex` 更新逻辑里要严格限制 `rf.Log[N].Term == rf.CurrentTerm`. 
- **坑点 4: `ApplyCh` 死锁**: 不要在 `rf.Mu.Lock()` 里面写向管道的读写: `rf.ApplyCh <- applyMsg`. 测试框架的 Tester 可能在读取 Channel 时想要访问节点的其他 RPC, 从而获取锁, 导致循环等待锁死. 正确做法是深度拷贝出待执行的 logs, 释放锁后再遍历执行 Channel 写入. 

---

## Part 3C: Persistence (持久化)

### 思路与具体内容
因为节点可能会崩溃重启, 必须提供可恢复的途径. Raft 白皮书明确指出, `CurrentTerm`、`VotedFor` 以及 `Log` 需要保存在不易失的稳定存储中. 当状态改变时调用 `persist()`, 节点启动恢复时调用 `readPersist()`. 

### 实现步骤
1. **数据序列化**: 使用课程提供的 `labgob.NewEncoder` 及 `labgob.NewDecoder`. 
2. **触发时机**: 并不是定时 persist, 而是在上述三个变量以及 Snapshot 信息 (`LastIncludedIndex`, `LastIncludedTerm`) 真正**发生变动**的时候实时调用 `persist()` 存盘保存. 
3. **容错读取**: 在 Make() 中起节点时调用 `readPersist()` 加载保存的数据. 

### 坑点
- **坑点 5: 漏写 `persist`**: 在极少数极端条件下, 比如网络落后导致日志被部分截断但没有新 Entry 追加, 但 `Log` 数组实际上发生了修改, 如果没有调用 `persist` 进行存盘, 后续 crash-recovery 的重启可能会出现数组越界或者日志不统一. 只要那几个核心字段被赋予新值或截断, 都必须立刻 `persist()`. 

---

## Part 3D: Log Compaction / Snapshots (日记压缩与快照)

### 思路与具体内容
Raft 运行久了, Log 会无限增长, 吃掉全部内存并拖慢启动时间. 解决方案是定期向 State Machine (KV-Server层) 请求打个镜像 Snapshot 把之前的日志删掉. 在持久化层面, `Snapshot` 和 Raft State 这两样东西被一起保存. 

### 实现逻辑与转变
由于删除了早期的日志, 导致数组的索引位和 `LogIndex` 发生了本质性脱离(从 1-based 变为了相对映射). 这是 Lab 3 中代码修改的一个主要痛苦来源. 

1. **引入 Dummy 位偏移系统**: 
   在任何时候需要通过 `LogIndex` 去拿 `LogEntry` 时, 真实寻找在数组的下标公式变为: `array_idx = Target_LogIndex - rf.LastIncludedIndex`. 
   为了保证索引不过界, 通常截断日志后会保留最早的一个位作为前哨(Dummy node), 这刚好也起到了 `PrevLogIndex`/`PrevLogTerm` 的记录作用: 这根独苗实际上持有了 `LastIncludedIndex` 和 `LastIncludedTerm`. 

2. **Snapshot() 生成快照**:
   上层 KV-Server 判断日志太大时会主动喂给 Raft 打好的字节流数据, 并且告知包含到哪个 Index. 
   - `rf.Log` 进行切片: 保留从被 Snapshot 之后的日志, 并丢弃头部的原日志. 
   - 更新 `rf.LastIncludedIndex` 与相应的 `Term`. 
   - 调用 `persist()` 将 `Snapshot[]byte` 与自身新状态一起刷进硬盘. 

3. **InstallSnapshot RPC**:
   当 Leader 准备向某一个 Follower 发送日志同步时, 发现该 Follower 所需要的起点 `NextIndex` 早已经被 Leader 做成 Snapshot 给截断丢弃掉了(`rf.NextIndex[i] <= rf.LastIncludedIndex`). 
   Leader 被迫改用发 `InstallSnapshot` RPC, 把整个 `Snapshot` 数据直接推给 Follower. 
   - Follower 收到快照, 首先根据 Term 判断合法性. 
   - 如果这个快照确实比本地所有的都已经 apply 的历史都新, 更新自己的 `Snapshot`. 
   - **重点**: 寻找能否对应保留还未被 Commit 或压缩的后续日记分支, 否则清空原日志重新构造 Dummy node. 将自身 `CommitIndex` 和 `LastApplied` 追平 `LastIncludedIndex`. 
   - 发送个特定信号或用 flag 使 Applier 协程能够知晓: `rf.ApplyCh <- ApplyMsg{SnapshotValid:true, ...}`. 上层发现后将完全重载自身 KV 字典恢复状态. 

### 细节与坑点
- **坑点 6: 真实 Index 与 相对 Index 换算的恐慌 (Panic)**
  在引入 Snapshot 的第一天, 你的系统在各种 RPC 的索引中极易炸出 `Index out of bounds`. 一定要将所有获取最后一条日志 index 和 term 的逻辑, 封装成 `getLastLogIndex()` 和 `getLastLogTerm()` 辅助函数, 集中处理数组长度判空和 `LastIncludedIndex` 相加的问题. 
- **坑点 7: 过期快照的乱序处理**
  `InstallSnapshot` 有可能通过网络延迟到达. 如果收到 RPC 通知要覆盖的 `LastIncludedIndex` **早于** 当前系统其实已经 apply 或者压缩好的 Index 时, 果断直接忽略它! 绝不允许让 `LastApplied` 发生开倒车的情况. 

---

## 总结: 关于并行和互斥锁的终极哲学

写全通过了这四个 Parts 的测试以后, 你会发现 Raft 调试的最头痛来源是**死锁**和**数据竞争 (Data Race)**. 

原则总结: 
1. **Never Sleep with a Lock**: 持有锁(`rf.Mu.Lock()`)的时间必须尽可能短. 千万不可持有着锁时发起 RPC 阻塞等待回应(`Call(...)`), 也不能在持锁期间将信道推入 `AppCh` 等待处理, 这会产生致命死锁链段. 
2. **The "Out-of-Lock" Gap**: 由于在发起远程调用的瞬间必须 `Unlock()` 锁, 在这个期间, 机器可能会收到别的 RPC 回调, 发生 Election Timeout, 或者由于网络延迟等收到回包时, 自己的状态已经经历了重大的改变(比如被抢走 Leader、重启、经过好几个 Term 推进). 所以一旦收回回包执行重新获取 `Lock()` 时, **第二句话必备的是确认身份和届数的合法性校验**: 
   ```go
   rf.Mu.Lock()
   if rf.CurrentTerm != args.Term || rf.State != StateLeader {
       rf.Mu.Unlock()
       return
   }
   ```
   这是一种典型的悲观锁范式补充, 能够完全杜绝由陈旧 RPC 回调带偏当前新局面的巨大灾难. 
3. **使用 `-race` 标定边界**: Go 提供的 `-race` 是你的好朋友. 只要你的逻辑在 Tester 下打出了 WARNING: DATA RACE, 不用怀疑, 必定是某一个后台读写没有带上互斥锁. 

希望这份笔记能帮助巩固这门极具挑战且收获丰厚的分布式存储课程核心. 
