# MIT 6.5840 Lab2: KV Server 与分布式锁的实现笔记

本次实验分为三个渐进的步骤: 首先是在内存中构建一个基于 RPC 通信的 KV Server; 其次是利用这个 KV Server 实现一个简单的分布式锁; 最后也是最具挑战性的一环, 是在模拟网络不稳定 (存在丢包、乱序、延迟等)的场景下, 保证这个分布式锁的安全性 (Safety)与活性 (Liveness).

## KV 服务器的搭建

1. 每个客户端通过封装好的 `Clerk` 与服务端通信, 底层基于 RPC 框架.
2. 客户端主要通过 RPC 向服务端发起两种操作: `Put(key, value, version)` 与 `Get(key)`.
3. 服务端在内存中维护一个 Map 结构, 为每一个 Key 记录一个 `(Value, Version)` 的元组.通过 Version (版本号)实现并发控制, 防止陈旧的请求覆盖较新的数据.
4. RPC 调用的返回值约定: 
   - `Get(key)`: 成功返回 OK, 如果键不存在则返回 `rpc.ErrNoKey`.
   - `Put(key, value, version)`: 成功返回 OK; 由于版本冲突修改失败返回 `rpc.ErrVersion`; 如果网络超时或丢包导致不确定请求是否在服务器端执行, 则返回 `rpc.ErrMaybe`.

### 服务端的实现

在 KV 服务器中, 核心任务是管理数据状态的读取与变更, 并保证在多线程并发访问时的并发安全.主要包括数据存储结构的定义, 以及处理客户端 RPC 调用 (`Get()` 和 `Put()`)的逻辑.

1. **服务端的结构体定义**: 
```go
type KVServer struct {
	mu sync.Mutex // 保护内存数据结构的互斥锁

	// data structure to store key-value pairs and their versions
	versions map[string]rpc.Tversion // 记录每个 Key 当前的 Version
	data     map[string]string       // 记录每个 Key 对应的 Value
}
```

2. **处理 `Get()` 请求**: 
服务端收到请求后加锁, 并在 `data` 字典中查找指定 `Key`.
```go
// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	value, exists := kv.data[args.Key]
	// if key does not exist, return ErrNoKey
	if !exists {
		reply.Err = rpc.ErrNoKey
		return
	}
	
	// key exists, return value and version
	reply.Value = value
	reply.Version = kv.versions[args.Key]
	reply.Err = rpc.OK
}
```

3. **处理 `Put()` 请求**: 
由于存在并发修改和网络重发, 服务端必须严格校验 `Version` 作为条件更新 (Compare-And-Swap 的微缩版).
```go
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	currentVersion, exists := kv.versions[args.Key]
	// 场景 1: key 不存在
	if !exists {
		// 如果要求插入的新版本为 0, 说明是初始写入, 合法
		if args.Version == 0 {
			kv.data[args.Key] = args.Value
			kv.versions[args.Key] = 1 // 写入后版本号变为 1
			reply.Err = rpc.OK
		} else {
			// 非法: 要求特定的前置版本, 但当前键尚不存在
			reply.Err = rpc.ErrNoKey
		}
		return
	}
	
	// 场景 2: key 存在, 需校验版本号是否匹配
	if args.Version != currentVersion {
		reply.Err = rpc.ErrVersion
		return
	}
	
	// 校验通过, 执行更新, 并将版本号 + 1
	kv.data[args.Key] = args.Value
	kv.versions[args.Key] = currentVersion + 1
	reply.Err = rpc.OK
}
```

### 客户端的实现

客户端 (`Clerk`)封装了网络通信的细节, 对外提供直观的 `Get` 和 `Put` 接口.由于网络环境不可靠, 客户端需要处理 RPC 超时 (`ok == false`)以及 `ErrMaybe` 这类表示“状态未知”的错误, 并进行重试.

1. **`Get()` 函数的实现**
```go
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	var reply rpc.GetReply // 每次 RPC 也可以重新声明局部变量
	
	// 使用无限循环, 直到拿到明确的返回结果 (成功或不存在)才退出
	for {
		ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
		// 网络通信正常
		if ok {
			return reply.Value, reply.Version, reply.Err
		}
		// 如果没有收到响应 (ok=false), 就说明可能丢包了, 循环重试
	}
}
```

2. **`Put()` 函数的实现**
与 `Get` 类似, 但当出现丢包 (`ok == false`) 导致需要重试时, 如果随后收到 `ErrVersion` 错误, 要敏锐地察觉这是否是因为自己的前置请求其实已经服务器执行了.
```go
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// 设置 RPC 调用的入参
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	var reply rpc.PutReply
	hadRetry := false
	
	for {
		// 发起 RPC 远程调用
		ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
		
		// ok == false 表示本次 RPC 失败 (如请求报文丢失或应答报文丢失)
		if !ok {
			hadRetry = true
			continue // 一直重试, 直到收到服务端的明确回应
		}
		
		// 收到明确回复, 检查是否存在版本冲突
		if reply.Err == rpc.ErrVersion {
			// 如果之前发生过重试, 而现在版本冲突了
			// 很有可能是由于之前丢失的应答报文其实在服务端已经成功执行过 Put 了
			// 此时返回 rpc.ErrMaybe 交给上层进行二次确认
			if hadRetry {
				return rpc.ErrMaybe
			}
			return rpc.ErrVersion
		}
		return reply.Err
	}
}
```

## 分布式锁的实现

在这个项目中, 我们用前面编写的 KV Server 作为底座 (类似单机版的 Redis 或者 Zookeeper), 在它之上利用原语和抽象来实现了一个分布式互斥锁 (Mutex).

### 分布式锁与单机锁的区别

- **单机锁 (如 sync.Mutex)**: 进程内的多线程通过共享同一块物理内存实现协作. 利用 CPU 的硬件级原子指令 (如 CAS)或者内核调度就能快速且相对确定地实现互斥.
- **分布式锁**: 作用于分布式系统网络中相互隔离的客户端进程. 它们不共享内存, 唯一交互和维持状态的途径就是通过网络去访问一个公共的服务端 (也就是我们的 KV Server). 然而网络具有不确定性 (丢包、延迟、乱序), 因此必须处理复杂的异常情况. 工业界的实际生产中, 为了应对持有锁的客户端宕机问题, 通常还需要引入**租约 (Lease)/过期 (Expiration)机制**, 但本 Lab 进行了简化, 不需要处理死锁恢复.

### 框架中 `lk.ck` 是如何运作的

在分布式锁的测试与使用场景中, 每一个欲获取锁的节点内部都会创建一个 `Lock` 对象: 
`lk := MakeLock(ck, "l")`
其中 `lk.l` 即将锁映射为 KV Server 上的某一个特定 `Key` (比如 `"l"`), 而 `lk.ck` 则是当前客户端的引用. 所以这里定义的这个分布式锁是不包含服务端的, 仅包含服务端上的特点的 `Key`, 以及客户端的引用.

**这个过程具体涉及到哪些组件交互？**
1. **接口抽象**: 在 `lock.go` 中, `lk.ck` 的类型是 `kvtest.IKVClerk` 接口.
2. **测试框架实例化**: 测试启动时, 底层框架会使用 `MakeClerk()` 构建真实的 `Clerk` 客户端, 将其转为 `IKVClerk` 后再塞给我们的 `MakeLock()`.
3. **RPC 通信屏蔽**: 由于客户端和 KV Server 不在同一进程, 当你调用 `lk.ck.Get(lk.l)` 时, 底层实际上是调用了我们在前面写的 `Clerk.Get()`, 把请求通过模拟网络环境序列化成数据包, 发给注册在远端的 `KVServer.Get`, 处理完后再把结果传输回来. 客户端在编写代码时感受到的只是本地函数调用.

### 客户端并发测试的流程梳理

代码通过以下流程模拟了大量客户端争抢锁: 
1. 测试入口 `runClients` 使用 `kvsrv.MakeTestKV` 初始化服务端.
2. `SpawnClientsAndWait` 批量生成很多异步的 Goroutine (`runClient`), 每个协程通过 `MakeClerk()` 创建自己独立的 RPC 客户端.
3. 这个客户端传到了 `oneClient` 函数中, 调用 `MakeLock(ck, "l")` 生成对资源 `"l"` 竞争的 Lock. 所有的客户端针对的是相同的 key `"l"`, 由此产生并发竞争.

### 网络不稳定下的并发抢锁问题

当存在网络不稳定时, 如果设计不当会由于重试产生巨大的隐患. 例如 **“重试虚假成功”** 现象: 
- 资源 `"l"` 处于空闲状态.
- A 和 B 同时 `Get("l")` 发现为空.
- A 和 B 同时发起 `Put("l", clientID, version=0)`.
- A 的 `Put` 请求到达了 Server 并成功占用. 但是**网络应答丢包返回给 A 的是失败 (超时)**.
- 这引发了 **Client_A 的重试 `Put`**. 此时原本属于 B 的较慢网络请求到达了 Server, 但因为版本已经被 A 改了, 所以 B 返回由于版本冲突 `ErrVersion` 而彻底失败.
- 此时重试出来的 A 的 Put 会发生什么？A 由于之前 `Put` 成功导致版本已经是 `1`, A 用 `version=0` 进行重试, 此时必然也会收到服务端的 `ErrVersion` 的应答.
- 若客户端 A 在它的 RPC 代理 `ck.Put()` 中仅仅处理为 `ErrVersion`, A 并不知道是因为它自己刚刚 Put 成功导致的这个版本错误, 就会认为抢锁失败而放行或阻塞. 因此我们在 `ck.Put()` 发现了 `hadRetry` 时, 必须返回 `ErrMaybe` 用于特殊标识.

## 分布式锁的代码实现

通过巧妙地结合循环验证: 先 `Get` 锁当前的状态和版本, 然后用携带自己独特签名 (ID)的方式执行带状态校验的 `Put`, 实现了安全可靠的资源抢占.

```go
func (lk *Lock) Acquire() {
	for {
		val, ver, err := lk.ck.Get(lk.l)
		// 如果返回 ErrNoKey, 说明之前该锁从未被触碰过, 处于原始状态
		if err == rpc.ErrNoKey {
			val = ""
			ver = 0
		}
		
		// 【防丢包关键点】If we already hold the lock (previous Put succeeded but response was lost)
		// 如果发现 KV Server 上的 val 等于我自己的 client ID, 
		// 就说明我在前一轮的 Put 请求其实已经抢到了锁, 只是刚好应答网络发生了丢包.
		if val == lk.id {
			return
		}
		
		// If the lock is free, try to acquire it
		if val == "" {
			err = lk.ck.Put(lk.l, lk.id, ver)
			if err == rpc.OK {
				return
			}
			// 如果返回 ErrMaybe: 前置的 Put 也许成功进了服务器, 只是没收到 OK, 因此循环回去走上面的 val == lk.id 判断.
			// 如果返回 ErrVersion: 意味着在咱们正要写入的时候, 有别人捷足先登更改了 Version 抢了锁, 只能循环回去从头再来.
		}
		// 退避等待, 防止死循环大量消耗网络 RPC 与 CPU
		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	for {
		val, ver, err := lk.ck.Get(lk.l)
		
		// 校验锁的持有者如果已经不是自己 (通常是因为自己前期已经成功 Release 过但丢失了应答导致重试), 则安全退出.
		if err == rpc.OK && val != lk.id {
			// Lock is not held by us - our release already succeeded
			return
		}
		
		// 如果的确仍锁定在自己名下, 释放自己！
		if err == rpc.OK && val == lk.id {
			// We still hold the lock, try to release it
			// 释放时将 val 写回空字符串 "", 版本加 1
			err = lk.ck.Put(lk.l, "", ver)
			if err == rpc.OK {
				return
			}
			// 一样要通过循环处理 ErrMaybe 异常
		}
		time.Sleep(100 * time.Millisecond)
	}
}
```

### 核心避坑要点: 
1. **先获取, 后更新**: 无论是 `Acquire()` 还是 `Release()`, 由于修改往往是有条件的, 因此通常必须先使用 `Get()` 获取最新的 `Version` 和状态 (谁占用的锁), 然后在发起 `Put(key, expected_version)`.
2. **避免脑裂/混乱**: 在发生重试时使用 `rand` 随机客户端唯一 `ID` 来标识锁此时正在被“谁”占有, 从而规避网络应答丢失造成的误判.如果不这样做, 两次相同参数的重试请求将难以区分究竟谁成功了.
3. **退避避让**: 如果抢锁失败不能立刻再次进行无间隔抢夺, 很容易打死模拟 RPC 网络与导致资源争用 (CPU 打满).因此通常在 `for` 中加上 `time.Sleep` 适当休眠.