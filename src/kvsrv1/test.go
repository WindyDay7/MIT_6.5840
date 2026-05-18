package kvsrv

import (
	// "log"
	"testing"

	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type TestKV struct {
	*kvtest.Test
	t        *testing.T
	reliable bool
}

func MakeTestKV(t *testing.T, reliable bool) *TestKV {
	// make a config with 1 server and 1 client, and use it to make a TestKV
	// The test will use the TestKV's MakeClerk() method to make a client, and the client will talk to the server in the config.
	cfg := tester.MakeConfig(t, 1, reliable, StartKVServer)
	ts := &TestKV{
		t:        t,
		reliable: reliable,
	}
	ts.Test = kvtest.MakeTest(t, cfg, false, ts)
	return ts
}

func (ts *TestKV) MakeClerk() kvtest.IKVClerk {
	clnt := ts.Config.MakeClient()
	ck := MakeClerk(clnt, tester.ServerName(tester.GRP0, 0))
	return &kvtest.TestClerk{ck, clnt}
}

func (ts *TestKV) DeleteClerk(ck kvtest.IKVClerk) {
	tck := ck.(*kvtest.TestClerk)
	ts.DeleteClient(tck.Clnt)
}
