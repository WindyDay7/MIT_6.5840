package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
)

type TaskState int

const (
	Idle       TaskState = iota // 初始状态, 还没分配
	InProgress                  // 已分配给某个 worker
	Completed                   // worker 报告完成
)

type TaskInfo struct {
	FileName  strings.Builder
	State     TaskState
	StartTime time.Time // 用于检测超时
}

type Coordinator struct {
	sync.Mutex
	mapTasks    []TaskInfo
	reduceTasks []TaskInfo
	nReduce     int
	TaskID      int // ID of the current Map task

	idleMapTasks    chan int // 存放 Idle map 任务的下标
	idleReduceTasks chan int // 存放 Idle reduce 任务的下标

	cond       *sync.Cond // Condition variable for signaling
	mapDone    bool       // Indicates if the Map phase is done
	reduceDone bool       // Indicates if the Reduce phase is done
}

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

// Get the Workers' reports about their progress
func (c *Coordinator) GetReport(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.Lock()
	defer c.Unlock()
	switch args.TaskType {
	case "Map":
		// A Map Task have been Completed
		c.mapTasks[args.TaskID].State = Completed
		for i := range args.IntermediateFiles {
			if args.IntermediateFiles[i] != "Empty" {
				// add the Task Process by Reduce to the Array
				c.reduceTasks[i].FileName.WriteString(args.IntermediateFiles[i] + ",")
			}
		}
		reply.Success = true
		// log.Printf("Map Phase Process the File %d completed.\n", args.WorkerID)
	case "Reduce":
		c.reduceTasks[args.TaskID].State = Completed
		reply.Success = true
	default:
		reply.Success = false
		log.Println("unknown task type in report")
		return errors.New("unknown task type in report")
	}
	return nil
}

// Done returns true if all tasks have been completed.
func (c *Coordinator) Done() bool {
	// log.Printf("Start Check if the Task Done\n")
	c.Lock()
	defer c.Unlock()
	// If reduce Done, All Task have been completed
	if c.reduceDone {
		return true
	}
	if !c.mapDone {
		// Check if a MapTask timeout
		// If a task timeout, this might because the worker down, put this task back
		for i := 0; i < len(c.mapTasks); i++ {
			if c.mapTasks[i].State == InProgress && time.Since(c.mapTasks[i].StartTime) > time.Second*10 {
				log.Printf("Map task %d timed out", i)
				c.mapTasks[i].State = Idle
				c.idleMapTasks <- i // Put the task back to idle channel
				c.cond.Broadcast()
			}
		}
		// Check if all Map Task Completed
		for _, t := range c.mapTasks {
			if t.State != Completed {
				return false
			}
		}
	}
	// log.Printf("Map Phase Have Done\n")
	if !c.mapDone {
		c.mapDone = true   // All map tasks are done, set the flag
		c.cond.Broadcast() // Notify all waiting goroutines
	}
	// Check if a ReduceTask Timeout
	for i := 0; i < len(c.reduceTasks); i++ {
		if c.reduceTasks[i].State == InProgress && time.Since(c.reduceTasks[i].StartTime) > time.Second*10 {
			log.Printf("Reduce task %d timed out", i)
			c.reduceTasks[i].State = Idle
			c.idleReduceTasks <- i // Put the task back to idle channel
		}
	}
	// Check if all Task Completed
	for _, t := range c.reduceTasks {
		if t.State != Completed {
			return false
		}
	}
	c.reduceDone = true
	return true
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	// Register the Coordinator as an RPC service
	rpc.Register(c)
	// Handle HTTP requests
	rpc.HandleHTTP()
	// Create a Unix socket for the Coordinator
	sockname := coordinatorSock()
	// Remove any existing socket
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	// Start serving RPC requests, Listen for incoming connections
	go http.Serve(l, nil)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Initialize the map and reduce tasks
	c.mapTasks = make([]TaskInfo, 0)
	c.reduceTasks = make([]TaskInfo, nReduce)
	c.idleMapTasks = make(chan int, len(files)) // Channel to store idle map tasks
	c.idleReduceTasks = make(chan int, nReduce) // Channel to store idle reduce tasks

	// Initialize the map tasks
	for i := range files {
		c.mapTasks = append(c.mapTasks, TaskInfo{
			State:     Idle,
			StartTime: time.Time{},
		})
		// Use strings.Builder for FileName
		c.mapTasks[i].FileName.WriteString(files[i])
		c.idleMapTasks <- i
	}
	// Initialize the Reduce Tasks
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = TaskInfo{
			State: Idle,
		}
		c.idleReduceTasks <- i
	}
	c.mapDone = false
	c.reduceDone = false
	c.cond = sync.NewCond(&c.Mutex)
	c.nReduce = nReduce
	c.server()
	return &c
}
