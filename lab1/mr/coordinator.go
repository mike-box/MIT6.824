package mr
import "log"
import "net"
import "os"
import "sync"
import "time"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// Your definitions here.
	mapTasks chan int
	reduceTasks chan int
	nReduce int
	nMap int
	mapRuning []int64
	reduceRuning []int64
	tasks []string
	mapCnt int
	reduceCnt int
	taskDone bool
	lock *sync.Cond 
}

// Your code here -- RPC handlers for the worker to call.
//
func (c *Coordinator) Request(args *ReqArgs, reply *ReplyArgs) error {
	// all task have been done
	c.lock.L.Lock()
	allDone := c.taskDone
	c.lock.L.Unlock()
	if allDone {
		reply.RepId = args.ReqId
		reply.RepOp = TaskDone
		return nil
	}

	switch args.ReqOp{
		case TaskReq:
			if len(c.mapTasks) > 0 {
				reply.RepId = args.ReqId
				reply.RepOp = TaskMap
				reply.RepTaskId = <-c.mapTasks
				reply.RepnMap = c.nMap
				reply.RepContent = c.tasks[reply.RepTaskId]
				reply.RepnReduce = c.nReduce
				c.lock.L.Lock()
				c.mapRuning[reply.RepTaskId] = args.ReqId
				c.lock.L.Unlock()
				go func(taskId int){
					time.Sleep(10*time.Second)
					c.lock.L.Lock()
					if c.mapRuning[taskId] != 1{
						c.mapTasks<-taskId
					}else{
						c.mapCnt--
					}
					c.lock.L.Unlock()
				}(reply.RepTaskId)
				return nil
			}else if len(c.mapTasks) == 0 {
				c.lock.L.Lock()
				mapCurr := c.mapCnt
				reduceCurr := c.reduceCnt
				c.lock.L.Unlock()

				if  mapCurr > 0 {
					reply.RepId = args.ReqId
					reply.RepOp = TaskWait
					return nil
				}else{
					if len(c.reduceTasks) > 0 {
						reply.RepId = args.ReqId
						reply.RepOp = TaskReduce
						reply.RepTaskId = <-c.reduceTasks
						reply.RepnMap = c.nMap
						reply.RepnReduce = c.nReduce
						c.lock.L.Lock()
						c.reduceRuning[reply.RepTaskId] = args.ReqId
						c.lock.L.Unlock()
						go func(taskId int){
							time.Sleep(10*time.Second)
							c.lock.L.Lock()
							if c.reduceRuning[taskId] != 1{
								c.reduceTasks<-taskId
							}else{
								c.reduceCnt--
								if c.reduceCnt == 0{
									c.taskDone = true
								}
							}
							c.lock.L.Unlock()
						}(reply.RepTaskId)
					}else{
						if reduceCurr > 0 {
							reply.RepId = args.ReqId
							reply.RepOp = TaskWait
						}
					}
				}
			}
		case TaskMapDone:
			c.lock.L.Lock()
			if c.mapRuning[args.ReqTaskId] == args.ReqId {
				c.mapRuning[args.ReqTaskId] = 1
			}
			c.lock.L.Unlock()
		case TaskReduceDone: 
			c.lock.L.Lock()
			if c.reduceRuning[args.ReqTaskId] == args.ReqId{
				c.reduceRuning[args.ReqTaskId] = 1
			}
			c.lock.L.Unlock()
		default:
			return nil
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.lock.L.Lock()
	res := c.taskDone
	c.lock.L.Unlock()
	return res
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapTasks = make(chan int,len(files))
	c.reduceTasks = make(chan int,nReduce)
	c.nReduce = nReduce
	c.nMap = len(files)
	c.mapRuning = make([]int64,len(files))
	c.reduceRuning = make([]int64,nReduce)
	c.tasks = make([]string,len(files))
	c.taskDone = false
	c.mapCnt = len(files)
	c.reduceCnt = nReduce
	c.lock = sync.NewCond(&sync.Mutex{})
	// each task will insert to the queue
	for i := 0; i < len(files) ; i++ {
		c.tasks[i] = files[i]
		c.mapRuning[i] = 0
		c.mapTasks <- i
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks <- i
		c.reduceRuning[i] = 0
	}

	// start RPC server
	c.server()
	return &c
}
