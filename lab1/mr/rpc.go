package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ReqArgs struct {
	ReqId int64
	ReqOp OpType
	ReqTaskId int
}

type ReplyArgs struct {
	RepId int64
	RepOp OpType
	RepTaskId int
	RepnMap int
	RepnReduce int
	RepContent string
}

type OpType int
const (
	TaskReq OpType = iota
	TaskMap 
	TaskReduce
	TaskMapDone
	TaskReduceDone
	TaskDone
	TaskWait
)

type TaskState int
const (
    Running TaskState = iota // value --> 0
    Stopped              // value --> 1
    Rebooting            // value --> 2
    Terminated           // value --> 3
)


// Add your RPC definitions here.
type Rpc struct {
	args  ReqArgs
	reply ReplyArgs
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
