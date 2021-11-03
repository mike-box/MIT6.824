package mr

import "fmt"
import "log"
import "io/ioutil"
import "net/rpc"
import "os"
import "time"
import "sort"
import "encoding/json"
import "hash/fnv"
import "strconv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by hash key.
type ByHashKey []KeyValue
func (a ByHashKey) Len() int           { return len(a) }
func (a ByHashKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByHashKey) Less(i, j int) bool { return ihash(a[i].Key) < ihash(a[j].Key)}

// for sorting by key.
type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
// key hash code
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
// process a map task
func startMapTask(timestamp int64,reply * ReplyArgs,mapf func(string, string) []KeyValue) bool {
	ifile, err := os.Open(reply.RepContent)
	defer ifile.Close()
	// read content from the file
	content, err := ioutil.ReadAll(ifile)
	if err != nil {
		log.Fatalf("can not read %v", reply.RepContent)
	}
	
	intermediate := mapf(reply.RepContent, string(content))
	//sort.Sort(ByHashKey(intermediate))
	ofile := make([]*os.File,reply.RepnReduce)
	for i := 0; i < reply.RepnReduce; i++ {
		ofname := "mr-" + strconv.Itoa(reply.RepTaskId) + "-" + strconv.Itoa(i)
		ofile[i], _ = os.Create(ofname)
		defer ofile[i].Close()
	}
	for _,kv := range intermediate{
		reduceId := ihash(kv.Key)%reply.RepnReduce
		enc := json.NewEncoder(ofile[reduceId])
		err := enc.Encode(&kv)
		if(err != nil){
			log.Fatalf("can not read %v", ofile[reduceId])
		}
	}
	//notice the server task finished
	args := ReqArgs{}
	args.ReqId = timestamp
	args.ReqOp = TaskMapDone
	args.ReqTaskId = reply.RepTaskId
	nextreply := ReplyArgs{}
	return call("Coordinator.Request", &args, &nextreply)
}

// process a reduce task
func startReduceTask(timestamp int64,reply * ReplyArgs,reducef func(string, []string) string) bool {
	// we check every intermediate map task file
	kva := []KeyValue{} 
	for i := 0; i < reply.RepnMap; i++ {
		ifilename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.RepTaskId)
		ifile, err := os.Open(ifilename)
		defer ifile.Close()
		// open file error
		if err != nil{
			log.Fatalf("Open File Error.")
		}
		// read all intermediate data from the file
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			} 
			kva = append(kva, kv)
		}
	}

	//store intermediate data to the out file
	sort.Sort(ByKey(kva))
	// write to the target file
	ofilename := "mr-out-" + strconv.Itoa(reply.RepTaskId)
	//fmt.Println("out file %v",ofilename)
	ofile,err := os.Create(ofilename)
	if err != nil{
		log.Fatalf("Creat Open File Error.")
	}
	defer ofile.Close()
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}

	//notice the server task finished
	args := ReqArgs{}
	args.ReqId = timestamp
	args.ReqOp = TaskReduceDone
	args.ReqTaskId = reply.RepTaskId
	nextreply := ReplyArgs{}
	return call("Coordinator.Request", &args, &nextreply)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {	
	for {
		timestamp := time.Now().Unix()
		args := ReqArgs{}
		args.ReqId = timestamp
		args.ReqOp = TaskReq
		reply := ReplyArgs{}
		ok := call("Coordinator.Request", &args, &reply)
		//we ask for a new task
		if reply.RepId != args.ReqId {
			//log.Fatalf("%v %v Coordinator Server has been closed.",reply.RepId,args.ReqId)
			return
		}
		if ok {
			switch reply.RepOp {
				case TaskMap: startMapTask(timestamp,&reply,mapf)
				case TaskReduce: startReduceTask(timestamp,&reply,reducef)
				case TaskWait: time.Sleep(time.Second)
				case TaskDone: return
				default: return
					
			}
		}else{
			log.Fatalf("Coordinator Server has been closed.")
			return
		}
		time.Sleep(time.Second)
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
