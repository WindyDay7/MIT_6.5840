package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	// data structure to store key-value pairs and their versions
	versions map[string]rpc.Tversion
	data     map[string]string
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		versions: make(map[string]rpc.Tversion),
		data:     make(map[string]string),
	}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
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

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	currentVersion, exists := kv.versions[args.Key]
	// key does not exist
	if !exists {
		// if version is 0, add the key and value, version becomes 1
		if args.Version == 0 {
			// install the value
			kv.data[args.Key] = args.Value
			kv.versions[args.Key] = 1 // initial version is 1
			reply.Err = rpc.OK
		} else {
			// key doesn't exist and version is not 0
			reply.Err = rpc.ErrNoKey
		}
		return
	}
	// key exists, check version
	if args.Version != currentVersion {
		reply.Err = rpc.ErrVersion
		return
	}
	// update value and version
	kv.data[args.Key] = args.Value
	kv.versions[args.Key] = currentVersion + 1
	reply.Err = rpc.OK
}

// Kill is called when the server is being shut down
func (kv *KVServer) Kill() {
	// Nothing to do for now
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
