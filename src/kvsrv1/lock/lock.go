package lock

import (
	"fmt"
	"math/rand"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	l  string
	id string // unique identifier for this lock client
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.l = l
	lk.id = fmt.Sprintf("%d", rand.Int63())
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		val, ver, err := lk.ck.Get(lk.l)
		if err == rpc.ErrNoKey {
			val = ""
			ver = 0
		}
		// If we already hold the lock (previous Put succeeded but response was lost)
		if val == lk.id {
			return
		}
		// If the lock is free, try to acquire it
		if val == "" {
			err = lk.ck.Put(lk.l, lk.id, ver)
			if err == rpc.OK {
				return
			}
			// ErrMaybe: might have succeeded, loop back and check with Get
			// ErrVersion: someone else got it, loop back
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		val, ver, err := lk.ck.Get(lk.l)
		if err == rpc.OK && val != lk.id {
			// Lock is not held by us - our release already succeeded
			return
		}
		if err == rpc.OK && val == lk.id {
			// We still hold the lock, try to release it
			err = lk.ck.Put(lk.l, "", ver)
			if err == rpc.OK {
				return
			}
			// ErrMaybe: might have succeeded, loop back and check with Get
		}
		time.Sleep(100 * time.Millisecond)
	}
}
