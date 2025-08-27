package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) error {

	for {
		// send a request to Coordinator for requesting a task
		args := RequestTaskArgs{}
		reply := RequestTaskReply{}
		args.WorkerID = os.Getpid() // use the process ID as WorkerID
		ok := call("Coordinator.IssueTask", &args, &reply)
		if !ok {
			log.Println("All tasks done, worker exit.")
			break
		}
		// Check the task type
		if reply.TaskType == "Map" {
			// Call the Map phase
			intermediate_filepaths := MapPhase(mapf, &reply)
			// Report the task status
			report_args := ReportTaskArgs{
				WorkerID:          args.WorkerID,
				TaskID:            reply.TaskID,
				Status:            "Completed",
				TaskType:          reply.TaskType,
				IntermediateFiles: intermediate_filepaths,
			}
			report_reply := ReportTaskReply{}
			task_complete := call("Coordinator.GetReport", &report_args, &report_reply)
			if !task_complete {
				log.Println("Report a Map Task Complete Failed, Worker exist")
				break
			}
		} else if reply.TaskType == "Reduce" {
			// Get a Reduce Task, Process the Reduce Task
			err := ReducePhase(reducef, &reply)
			if err != nil {
				log.Println("Worker Reduce Phase Failed, Break")
				break
			}
			// Report complete a Reduce Task
			report_args := ReportTaskArgs{
				WorkerID: args.WorkerID,
				TaskID:   reply.TaskID,
				Status:   "Completed",
				TaskType: reply.TaskType,
			}
			report_reply := ReportTaskReply{}
			task_complete := call("Coordinator.GetReport", &report_args, &report_reply)
			if !task_complete {
				log.Println("Report a Reduce Task Complete Failed, Worker exist")
				break
			}
		} else {
			// No Task Now, Workrt will wait for a second
			time.Sleep(1 * time.Second)
		}
	}
	return nil
}

// MapPhase Process a Map Task
func MapPhase(mapf func(string, string) []KeyValue, reply *RequestTaskReply) []string {
	filename := reply.FileName
	// process the reply.
	unread_file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	// read the file content
	content, err := io.ReadAll(unread_file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	unread_file.Close()
	// Call the Map function
	kva := mapf(filename, string(content))
	// Save the intermediate results
	intermediate_filepaths := SaveInterResult(kva, reply)
	return intermediate_filepaths
}

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

func SaveInterResult(kva []KeyValue, reply *RequestTaskReply) []string {
	var intermediate_filepaths []string
	// split the intermediate key-value pairs into buckets
	buckets := make([][]KeyValue, reply.NReduce)
	for _, kv := range kva {
		// log.Printf("key=%v, NReduce=%v\n", kv.Key, reply.NReduce)
		if reply.NReduce == 0 {
			log.Fatal("NReduce is 0, cannot proceed")
		}
		y := ihash(kv.Key) % reply.NReduce
		buckets[y] = append(buckets[y], kv)
	}

	// write the buckets into temporary files then rename it to final names
	for y := 0; y < reply.NReduce; y++ {
		if len(buckets[y]) == 0 {
			intermediate_filepaths = append(intermediate_filepaths, "Empty")
			continue
		}
		// recored the file_paths of the intermideate files by MAP Phase
		intermediate_filepaths = append(intermediate_filepaths, fmt.Sprintf("mr-%d-%d.json", reply.TaskID, y))
		// use the temporary file to write all kv pairs for this buckets
		// avoid the Reduce Function read incomplete files
		tmp, err := os.CreateTemp(".", fmt.Sprintf("mr-%d-%d-*.json", reply.TaskID, y))
		if err != nil {
			log.Fatalf("cannot create temp file for mr-%d-%d: %v", reply.TaskID, y, err)
		}
		defer os.Remove(tmp.Name())
		// write all kv pairs for this bucket
		enc := json.NewEncoder(tmp)
		for _, kv := range buckets[y] {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("cannot encode kv to temp file: %v", err)
			}
		}
		if err := tmp.Close(); err != nil {
			log.Fatalf("close temp file error: %v", err)
		}

		final := fmt.Sprintf("mr-%d-%d.json", reply.TaskID, y)
		if err := os.Rename(tmp.Name(), final); err != nil {
			log.Fatalf("rename %s -> %s error: %v", tmp.Name(), final, err)
		}

	}
	return intermediate_filepaths
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong. the args and reply must be pointers.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
