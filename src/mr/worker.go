package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// rename function
func finalizeReduceFile(tmpFile string, taskN int) {
	finalFile := fmt.Sprintf("mr-out-%d", taskN)
	os.Rename(tmpFile, finalFile)
}
func getIntermediateFile(mapTaskN int, redTaskN int) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskN, redTaskN)
}

func finalizeIntermediateFile(tmpFile string, mapTaskN int, redTaskN int) {
	finalFile := getIntermediateFile(mapTaskN, redTaskN)
	err := os.Rename(tmpFile, finalFile)
	if err != nil {
		log.Print("rename error")
		return
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	for {
		ok, res := GetMapTask()
		if !ok || res.Finished {
			break
		}

		// Wait if master is waiting for all files to be mapped
		if res.Empty {
			continue
		}

		args := &CommitArgs{}
		args.Type = res.Type
		args.ID = res.Index

		switch res.Type {
		case Map:
			if err := mapping(mapf, res.Filename, res.NReduce, res.Index); err != nil {
				log.Fatal(err)
			}
		case Reduce:
			if err := reducing(reducef, res.NReduce, res.Index); err != nil {
				log.Fatal(err)
			}
		}
		commit(args)
	}
}

func commit(args *CommitArgs) {
	replys := CommitReply{}

	call("Coordinator.Commit", &args, &replys)
}

func mapping(mapf func(string, string) []KeyValue, filename string, nReduce, index int) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return fmt.Errorf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	sort.Sort(ByKey(kva))

	var fileBucket = make(map[int]*json.Encoder)
	for i := 0; i < nReduce; i++ {
		oname := fmt.Sprintf("mr-%d-%d", index, i)
		ofile, _ := os.Create(oname)

		fileBucket[i] = json.NewEncoder(ofile)
		defer ofile.Close()
	}

	for _, v := range kva {
		reduceID := ihash(v.Key) % nReduce

		// 写入文件 mr-X-Y 中
		enc := fileBucket[reduceID]
		err := enc.Encode(&v)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetMapTask ...
func GetMapTask() (bool, *AssignReply) {
	args := AssignArgs{}

	replys := AssignReply{}

	if !call("Coordinator.Assign", &args, &replys) {
		return false, nil
	}

	return true, &replys
}

func reducing(reducef func(string, []string) string, nReduce, index int) error {

	kva := make([]KeyValue, 0)
	for i := 0; i < nReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, index)

		file, _ := os.Open(filename)

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))

	outFilename := fmt.Sprintf("mr-out-%d", index)
	ofile, _ := os.Create(outFilename)

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

	return nil
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
