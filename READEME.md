# MIT 6.5840 - Spring 2025 Lab 1: MapReduce

这是我实现 MIT 6.5840 的笔记, 希望对大家有帮助

## MapReduce 算法

MapReduce 是一个分布式数据处理算法, 在我们平时写一些小程序往往不会接触分布式算法和应用, 一方面是后台的抽象, 我们使用的所有系统, 访问的所有网页, 后台是透明, 而对一些小程序而言, 则数据量太小, 不需要分布式. 当接触生产系统后, 才发现, 分布式是极其重要的控制算法, 是一个系统稳定运行的最重要的保障.

MapReduce 的核心思想是基于分而治之的策略(divide-and-conquer). 它包含两个重要的步骤, **Map** 与 **Reduce**.

### MapReduce 的工作流

MapReduce 算法的工作流与框架图如下图所示:

![img](<https://img2024.cnblogs.com/blog/1346871/202508/1346871-20250806171831181-2037035419.svg> =800x800)

在接收到用户请求或者输入后, MapReduce 算法的执行分为下列三个步骤:

1. Map 阶段: 处理输入的数据, 并生成中间的 `<Key, Value>` 对, 用于存储中间处理结果.
2. Shuffle & Sort 阶段: 将中间结果按照键值对的 Key 进行分组.
3. Reduce 阶段: 聚合分组后的结果, 并生成最后的输出.

MapReduce 通常是用来处理大量数据的场景, 我们使用计算单词出现的频率作为例子, 来讲述 MapReduce 算法.

#### Step1 Input Splitting

1. 输入数据是存储在分布式文件系统中的文件, 例如(HDFS in Hadoop)
2. 这些文件数据会被分为块, 通常是 64MB 或者 128MB 大小, 作为 Map 函数的输入.
3. 每一个数据块会在不同的节点上并行执行.

#### Step 2: Map Phase (Mapping Function)

1. 每个工作节点上会运行着用户自定义的 Map 函数, 如图中所示, 用来处理输入文件
2. Map 函数处理输入后会得到中间结果, 以 `<Key, Value>` 的形式表示, 并将中间结果写入本地磁盘中
3. `<Key, Value>` 对的结果是用户自定义的, 可以根据 Map 函数具体生成

在单词计数的例子中, 假设我们的输入文件是: `hello world hello`.
Map 函数生成的中间结果可以是下面的结构:

```css
("hello", 1)
("world", 1)
("hello", 1)
```

#### Step 3: Shuffle & Sort Phase

1. Reduce 节点会从 Coordinator 获取所有 Map 任务生成的中间文件的位置, 然后主动通过 RPC 拉取(Remote Read) 的方式读取这些文件.
2. Reduce 节点会整理读取到的 `<Key, Value>` 对, 将所有来自不同 Map Task 的中间 `<Key, Value>` 按照 Key 值整理合并在一起.
3.在所有数据都拉取完毕后, Reduce Worker 对收到的中间 `<Key, Value>` 对按 key 排序.

例如, 上述的单词计数的中间结果如下:

```css
Intermediate Output:
("hello", [1, 1])
("world", [1])
```

#### Step 4: Reduce Phase (Reducing Function)

1. Reduce 节点会使用用户自定义的函数(Reducing Function) 来整理与计算最终结果.

例如单词计数的程序会生成下面的结果:

```css
("hello", 2)
("world", 1)
```

我们这里只是介绍了一个简单的例子, Map Reduce 算法的核心是分治, 如果一个任务可以将执行过程划分为 Map 和 Reduce 阶段, 那么该任务就可以使用 Map Reduce 算法, 例如分布式数据库, 或者上述不是对单词计数, 而是找出出现频率最大的单词, 只需要修改 Reduce 算法即可.

### Coordinator 节点的作用

前面我们讲的是 Map Reduce 算法的核心思想, 在[图一](https://img2024.cnblogs.com/blog/1346871/202508/1346871-20250806171831181-2037035419.svg) 中可以看到 Coordinator 节点在实际实现 Map Reduce 算法框架中起到重要作用.

在几乎所有的分布式框架中, 我们都需要一个中央节点来维持系统的稳定与正确运行. 在 MapReduce 算法实现中, 这里的 Coordinator 节点就是中心节点, 并且起到了协调计算的作用. 简单点说, Coordinator 节点主要作用是: 分配任务, 进程监控, 处理失败, 协调 Map 工作节点与 Reduce 工作节点, 并相互通信.

#### Coordinator 节点的主要任务

Coordinator 节点的具体任务依赖于框架的实现方式, 这里我们列出了一些常见的主要任务, 并对这些任务进行分类如下:

1. 对输入的分类: Coordinator 节点会将输出文件分成大小相同的块.
2. 调度 Map Node 节点分配任务: 对每一个块, Coordinator 会采用调度算法为其分配一个 Map 节点, 执行 Map 函数.
3. 跟踪节点的状态: Coordinator 节点会跟踪所有节点的状态, 例如, 空闲, 正在运行, 或者运行完成
4. 管理工作节点: 这一步骤十分重要, Coordinator 会维护一个正在工作的节点的列表, 通常, 它会周期性的检查(Ping或者发送心跳) 来检查这些节点是否存活, 以及检查是否出现了工作失败的节点.
5. 处理 Map 节点的输出: Map 节点与 Reduce 节点并不直接通信, Map 节点将 Intermediate 结果写到磁盘后, Coordinator 会记录这些 Locations, 然后将这些 Locations, 发送给 Reduce 节点, 通知 Reduce 节点从这些 Location 读取内容.
6. 调度 Reduce 节点: 在所有的 Map Node 完成 Task 之后, Coordinator 节点通知 Reduce 节点开始工作, 但是 Reduce Node 实际上通过 RPC Remote Read 的方式读取中间结果.
7. 失败重启: 如果一个 Map Node 失败了, Coordinator 为重新分配一个新的 worker 来执行这个 split, 如果一个 Reduce 节点失败了, Coordinator 会告知它从失败的地方重新开始运行.

这些是 Coordinator 节点的常见功能, 在 Lab1 中, 有些需要实现, 有些则不需要实现.

## Lab1

我还是习惯先把我的理解与看完 Lab1 的这些描述记录下来, 然后再开始写代码, 后续也都会补充进来.

### Getting started

在正式开始任务之前, 我们需要做一些准备工作, 以及了解一些基础背景, MapReduce 算法我们已经在前面介绍过了, 就不在这里赘述了.

在 `src/main/mrsequential.go` 中提供了一个简单的序列化实现的 MapReduce. 可以看一下这部分代码, 这其中和并行模式的, 分布式的 MapReduce 算法差别最大的地方是, MAP 和 Reduce 过程是顺序执行的, 并且中间结果, `intermediate` 是存储在内存中直接供 Reduce 节点使用的.

还有需要知道 Go 语言中, 动态链接库的使用方式, 在 `mrapps` 目录下提供了一些 MapReduce 的应用, 我们首先要将这些应用编译成 Go 语言中的 plugin, 然后使用他们. Go 中 plugin 使用的方式在这里就不过多叙述了, 感兴趣的话可以去了解一下:

因此我们可以使用下面的方式来测试

```shell
$ cd ~/6.5840
$ cd src/main
$ go build -buildmode=plugin ../mrapps/wc.go
$ rm mr-out*
$ go run mrsequential.go wc.so pg*.txt
$ more mr-out-0
A 509
ABOUT 2
ACT 8
...
```

### 任务简介

在 Lab1 中我们需要实现一个分布式的 MapReduce 系统, 这个框架结构与谷歌论文中, 也就是前面的图一中的框架类似, 它包含两个进程, Coordinator 进程和 Worker 进程. 系统中只有一个 Coordinator 进程作为 Coordinator 的角色, 有一个或者多个 Workers 进程, 在实际生产中, 这些服务会运行在不同的机器上, 但是现在他们运行在同一台机器上, 并使用 RPC 来通信.

每一个 Worker 进程会不断的向 Coordinator 进程(Coordinator 节点) 申请任务, 然后读取任务的输出, 并执行, 将结果写入到输出文件中. Coordinator 进程(Coordinator 节点) 也需要完成低级的监控功能, 需要在限定的时间(10s) 内判断 Worker 进程是否已经完成任务, 如果没有完成, Coordinator 会将该 Task 分配给其他进程.

### 一些规则

1. 每一个 Map Worker 在 Map 阶段结束后在存储中间结果的时候需要将 intermediate result 分给 `nReduce` Reduce 任务, 需要需要生成 `nReduce` 个块来存储中间结果. `nReduce` 是系统中 Reduce Worker 的个数.
2. Worker 进程需要将第 X 个 Reduce Task 的输出写到文件 mr-out-X 中, mr-out-X 文件的格式应该是每一个 Reduce 函数的输出包含一行, 格式固定, 如下: `fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)`.
3. Map Worker 输出的中间结果应该存储在当前目录下, 以供 Reduce 函数读取.
4. 在 `main/mrCoordinator.go` 中会周期的检查 `mr/Coordinator.go` 中 `Done()` 函数的返回值, `Done()` 返回 True 表示 MapReduce 任务完全结束, 系统退出.
5. 系统中 MapReduce Job 结束后, 每个 Worker 节点的工作进程也需要退出, 一个简单的实现方式就是使用 RPC 的 `call()` 返回值, 当 Worker 节点连接不上 Coordinator 节点的时候, 说明 Job 已完成, Worker 节点退出.

### Hints

在具体实现的时候, Lab1 提供了一些思路, 根据这些思路很容易完成 Lab1 的实现部分.

1. 该系统启动的一个方式就是修改 `mr/worker.go` 中的 `Worker()` 函数, `Worker()` 函数启动的时候通过 RPC 向 Coordinator 节点 Coordinator 申请一个 Task. 然后修改 `Coordinator` 返回一个未执行的文件, 然后修改 `Worker` 读取返回的文件, 然后调用 Map 函数.
2. 该系统使用 Go 语言中 plugin 调用 Map 与 Reduce 函数, 并且依赖所有的 Workers 共享一个文件系统, 我家有三台电脑, 我装了一个共享磁盘尝试了一下是可以的.
3. Map 函数生成的 `intermediate result` 的合理命名是 x, 其中 X 是 Map Task 的编号, Y 是 Reduce Task 的编号.
4. `intermediate result` 文件的存储形式建议使用 Json 格式存储, 使用 Go 语言中的 `encoding/json` 包即可. 它的具体使用我们在代码中体现.
5. 在 Worker 中可以通过 `ihash(key)` 函数, 选择对应的 Reduce 节点执行.
6. Coordinator, 也就是 Coordinator 节点中的函数, 是并发执行的, 需要考虑加锁
7. 在 MapReduce 中, Reduce 任务必须等到 Map 全部完成后才能执行. 我们可以让 worker 端定时去问 Coordinator 是否存在任务(time.Sleep + 轮询), 也可以让 Coordinator 端在处理 Reduce 请求的 RPC handler 里卡住等待(time.Sleep 或 sync.Cond), 等条件满足才返回任务(使用条件变量等待的机制). Go 的 RPC 框架会为每个 RPC 请求单独起一个 goroutine, 所以即使一个请求在等待, 不会影响其他 RPC 的处理.
8. 在这个 Lab 中, Coordinator 分配给 worker 一个任务后, 最多等待 10s, 10s 之后判断该节点为 die, 无法执行任务.
9. 为了测试 wroker crash 之后重启与重新执行任务的功能, 可以使用 `mrapps/crash.go`, 让 worker 随机 crash.
10. Map 函数会将结果写到文件中, Reduce 函数会读取临时的结果, 为了避免读错, 以及 Map 函数 Crash 的情况下, Reduce 函数仍然把文件读取了, 如果先命名, 再写文件就会导致这种现象. 因此建议使用写临时文件, 然后重命名的方式, 这样重命名之后的文件一定是完整的文件.
11. 在 Go RPC 里, 传输的结构体必须用 首字母大写字段, 否则 RPC 根本不会传这个字段. 调用 RPC 前, reply 一定要用全默认值初始化, 不要预设任何字段, 否则可能会出现不报错但数据错误的情况.

### 需要考虑的几个问题

1. 一个 Worker 节点有三项重要的工作, 分别是 , Map 函数执行, Reduce 函数执行, 与 Coordinator 通信, RPC 调用.
2. Worker 和 Coordinator 之间通信的时候, 使用一种结构体作为报文, 还是说不同的函数使用不同的结构体作为报文呢, 例如 Map 请求 Task 的时候, 使用一种结构体, Reduce 请求 Task 的时候, 使用另一种结构体, 是否需要这样考虑.
3. Worker 和 Coordinator 之间通信与执行的顺序是: 首先 Worker 向 Coordinator 发送 Task 请求, Coordinator 会分配一个任务给 Worker
4. 对于 Coordinator 来说, 一个文件被处理完, 是 Worker 返回 Report Task Completed 之后, 而不是 Issue Task 之后, Issue Task 表示该文件已经被分配, 但是并没有被处理完.

## 实现方式

我总结我实现中的一些细节, 以及容易出错的点如下:

1. Worker 执行的主要步骤分别是, 向 Coordinator 请求任务, 执行 Map Phase 或者 Reduce Phase, 最后向 Coordinator Report 任务完成.
2. Worker 在向 Coordinator 请求任务的时候, 由 Coordinator 决定返回 Map 任务还是 Reduce 任务.
3. Coordinator 收到 Worker 申请 Task 的请求时, 首先分配 Map Task, Map Task 分配完后再分配 Reduce Task. Coordinator 分配 Reduce Task 的时候必须首先 Wait(), 等待所有 Map Task 完成.
4. Coordinator 每隔一秒钟检查是否所有任务完成, 以及是否存在 Worker 超时, 如果 Worker 超时, 将分配给该 Worker 的 Task 标记为未分配状态, 下一次重新分配.

### Coordinator 的实现

下面是我觉得 Coordinator 中最重要的步骤, 也就是分配任务步骤的实现.

```go
// The RPC function for worker to call, issue tasks for the worker
func (c *Coordinator) IssueTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.Lock()
	defer c.Unlock()
	// Issue the Map Task firstly
	if len(c.idleMapTasks) != 0 {
		taskID := <-c.idleMapTasks
		reply.TaskID = taskID
		reply.TaskType = "Map"
		reply.NReduce = c.nReduce
		reply.FileName = c.mapTasks[taskID].FileName.String()
		c.mapTasks[taskID].State = InProgress
		c.mapTasks[taskID].StartTime = time.Now()
		// log.Printf("Coordinator Issue Map Task %d to Worker %d\n", taskID, args.WorkerID)
		return nil
	} else if len(c.idleReduceTasks) != 0 {
		// If MapPhase haven't done, wait
		if !c.mapDone {
			// log.Printf("Reduce Start Wait for Map Done")
			c.cond.Wait()
		}
		if !c.mapDone {
			reply.TaskType = "Wait"
			return nil
		}
		taskID := <-c.idleReduceTasks
		// log.Printf("Coordinator Issue Reduce Task %d to Worker %d\n", taskID, args.WorkerID)
		// The TaskID is the index of reducetasks
		reply.TaskID = taskID
		reply.TaskType = "Reduce"
		reply.FileName = c.reduceTasks[taskID].FileName.String()
		c.reduceTasks[taskID].State = InProgress
		c.reduceTasks[taskID].StartTime = time.Now()
		return nil
	} else {
		if c.reduceDone {
			reply.TaskType = "Exit"
			return errors.New("all Task have been completed, Worker Exit")
		}
		reply.TaskType = "Wait"
		return nil
	}
}
```

worker 中的步骤我觉得还稍微简单一些, 按照 Reduce Phase 和 Map Phase 调用对应的 Map 函数与 Reduce 函数即可, 我列出了我实现的 Reduce Phase 如下:

```go
// ReducePhase Process a Reduce Task
func ReducePhase(reducef func(string, []string) string, reply *RequestTaskReply) error {
	// delete the final ",", and split by ","
	files := strings.TrimSuffix(reply.FileName, ",")
	// log.Printf("Reduce Phase Will Process Files as %s", reply.FileName)
	intermediate_files := strings.Split(files, ",")
	if len(intermediate_files) == 0 {
		log.Printf("No intermediate files for Reduce Task %d", reply.TaskID)
		return nil
	}
	// 使用 map 保存 Key -> []Values, 作为 Reduce 函数的输入
	result := make(map[string][]string)
	// these intermediate_files have the same suffix but different prefix
	for _, intermediate_file_path := range intermediate_files {
		// log.Printf("Reduce Phase Start Processing File %s\n", intermediate_file_path)
		// Process a intermediate file, a intermediate file contain many Key-Values
		if intermediate_file_path == "" {
			continue
		}
		intermediate_file, err := os.Open(intermediate_file_path)
		if err != nil {
			return fmt.Errorf("cannot open the intermediate file %s", intermediate_file_path)
		}
		defer intermediate_file.Close()
		dec := json.NewDecoder(intermediate_file)
		// 反序列化每一行的 KeyValue 值, 然后按照 Key 值进行分组
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			// 把 kv.Value 添加到 result[kv.Key] 的切片里
			result[kv.Key] = append(result[kv.Key], kv.Value)
		}
	}
	// create the final output file by reduce phase
	oname := fmt.Sprintf("mr-out-%d", reply.TaskID)
	// log.Println("Creating the Reduce output file:", oname)
	ofile, _ := os.Create(oname)
	// Write the final result into the file
	for key, values := range result {
		output := reducef(key, values)
		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}
	ofile.Close()
	return nil
}
```

## BUG 记录

1. 很多变量初始化与赋值错误, 初始化 Map 任务的时候写成了 `c.mapTasks = make([]TaskInfo, 0)`, 后面又使用了 append 导致错误, Map 任务完成之后, 没有设置该 Task 的 Completed 标识, AI 帮忙完成的小 BUG 很多
2. Reduce Phase 的时候, 由于 ihash() 阶段处理键值对的随机性, 某个 Reduce Task 可能没有分配到需要处理的文件, 这时, 该 Reduce Task 应该是直接完成, 而不是因为读取不到文件而结束 Worker 进程.
3. Coordinator 中所有任务都分配完成, 不能直接返回没有任务执行, 让 Worker 退出, 因为分配完成的 Task 可能因为超时或者某个 Worker Crash, 导致该 Task 没有被成功执行, 该 Task 还会返回任务队列, 重新执行.
4. 当 Coordinator 分配完所有的 Map Task 之后开始分配 Reduce Task, 会进入 Wait(), 如果此时, 正在执行 Map Phase 的 Worker Crash 了, 那么 Coordinator 检测到该 Worker 超时, 应该通知分配任务的 goroutine 跳出 Wait, 重新分配.
