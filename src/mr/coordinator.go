package mr

import (
	"context"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	TaskInit = iota
	TaskRunning
	TaskDone
)

type Coordinator struct {
	// Your definitions here.

	// protect coordinator state
	// set to true when all reduce tasks are complete
	files      []string
	nReduce    int
	mapTask    []int
	reduceTask []int
	lock       sync.Mutex
	timeout    time.Duration
	done       bool
}

// Your code here -- RPC handlers for the worker to call.

//
// Handle GetTask RPCs from worker
//
// Assign dispatch task to specific worker
func (m *Coordinator) Assign(args *AssignArgs, reply *AssignReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	ctx, _ := context.WithTimeout(context.Background(), m.timeout)

	if m.assignMapTask(ctx, args, reply) {
		return nil
	}

	for _, v := range m.mapTask {
		if v != TaskDone {
			reply.Empty = true
			return nil
		}
	}

	if m.assignReduceTask(ctx, args, reply) {
		return nil
	}

	for _, v := range m.reduceTask {
		if v != TaskDone {
			reply.Empty = true
			return nil
		}
	}
	m.done = true
	reply.Finished = true

	return nil
}

func (m *Coordinator) assignMapTask(ctx context.Context, args *AssignArgs, reply *AssignReply) bool {
	for id, v := range m.mapTask {
		if v == TaskRunning || v == TaskDone {
			continue
		}

		//fmt.Printf("A worker is requiring a map task\n")

		reply.Filename = m.files[id]
		reply.Index = id
		reply.NReduce = m.nReduce
		reply.Type = Map

		m.mapTask[id] = TaskRunning

		go func(c context.Context) {
			select {
			case <-ctx.Done():
				m.lock.Lock()
				defer m.lock.Unlock()

				if m.mapTask[id] == TaskRunning {
					m.mapTask[id] = TaskInit
					log.Printf("%v map task timeout\n", m.files[id])
				}
			}
		}(ctx)

		return true
	}

	return false
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.done
}

func (m *Coordinator) assignReduceTask(ctx context.Context, args *AssignArgs, reply *AssignReply) bool {
	for id, v := range m.reduceTask {
		if v == TaskRunning || v == TaskDone {
			continue
		}

		//fmt.Printf("A worker has require a reduce task\n")
		m.reduceTask[id] = TaskRunning

		reply.Index = id
		reply.NReduce = m.nReduce
		reply.Type = Reduce

		go func(c context.Context) {
			select {
			case <-ctx.Done():
				m.lock.Lock()
				defer m.lock.Unlock()

				if m.reduceTask[id] == TaskRunning {
					m.reduceTask[id] = TaskInit
					log.Printf("%v reduce task timeout\n", id)
				}
			}
		}(ctx)

		return true
	}

	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func (m *Coordinator) Commit(args *CommitArgs, reply *CommitReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	switch args.Type {
	case Map:
		//fmt.Printf("%v has been mapped\n", m.files[args.ID])
		m.mapTask[args.ID] = TaskDone
	case Reduce:
		//fmt.Printf("mr-out-%v has been reduced\n", args.ID)
		m.reduceTask[args.ID] = TaskDone
	}

	return nil
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	m := &Coordinator{}
	m.files = files
	m.nReduce = nReduce
	m.mapTask = make([]int, len(files))
	m.reduceTask = make([]int, nReduce)
	m.done = false
	m.timeout = time.Second * 10
	m.server()
	return m
}
