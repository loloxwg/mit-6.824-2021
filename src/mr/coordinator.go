package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.

	// protect coordinator state
	// from concurrent access
	mu sync.Mutex
	//
	cond *sync.Cond
	// len(mapFiles) == nMap
	mapFiles     []string
	nMapTasks    int
	nReduceTasks int

	// Keep track of  when tasks are assigned
	// and which tasks have finished
	mapTasksFinished    []bool
	mapTasksIssued      []time.Time
	reduceTasksFinished []bool
	reduceTasksIssued   []time.Time

	// set to true when all reduce tasks are complete
	isDone bool
}

// Your code here -- RPC handlers for the worker to call.

//
// Handle GetTask RPCs from worker
//
func (c *Coordinator) HandlerGetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.NReduceTasks = c.nReduceTasks
	reply.NMapTasks = c.nMapTasks
	// TODO issue all map and reduce tasks
	for {
		mapDone := true
		for m, done := range c.mapTasksFinished {
			if !done {
				if c.mapTasksIssued[m].IsZero() || time.Since(c.mapTasksIssued[m]).Seconds() > 10 {
					reply.TaskType = Map
					reply.TaskNum = m
					reply.MapFile = c.mapFiles[m]
					c.mapTasksIssued[m] = time.Now()
					return nil
				} else {
					mapDone = false
				}
			}
		}
		// if all maps are
		if !mapDone {
			//
			c.cond.Wait()
		} else {
			// we are done all map tasks
			break
		}
	}
	// all map tasks are done ,issue reduce tasks Now
	for {
		redDone := true
		for r, done := range c.reduceTasksFinished {
			if !done {
				if c.reduceTasksIssued[r].IsZero() || time.Since(c.mapTasksIssued[r]).Seconds() > 10 {
					reply.TaskType = Reduce
					reply.TaskNum = r
					c.reduceTasksIssued[r] = time.Now()
					return nil
				} else {
					redDone = false
				}
			}
		}
		if !redDone {
			//
			c.cond.Wait()

		} else {
			// we are done all reduce tasks
			break
		}
	}
	// if all reduce tasks are done ,send the querying worker
	// a Done TaskType ,and send isDone to true.
	reply.TaskType = Done
	c.isDone = true
	return nil
}

func (c *Coordinator) HandlerFinishedTask(args *FinishedTaskArgs, reply *FinishedTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case Map:
		c.mapTasksFinished[args.TaskNum] = true
	case Reduce:
		c.reduceTasksFinished[args.TaskNum] = true
	default:
		log.Fatalf("Bad finished task? %v", args.TaskType)
	}

	// wake up the GetTask handler loop: a task has finished,so we might be able to assign another one
	c.cond.Broadcast()

	return nil
}

//
//
//

//
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.isDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.cond = sync.NewCond(&c.mu)

	c.mapFiles = files
	c.nMapTasks = len(files)
	c.mapTasksFinished = make([]bool, len(files))
	c.mapTasksIssued = make([]time.Time, len(files))

	c.nReduceTasks = nReduce
	c.reduceTasksIssued = make([]time.Time, nReduce)
	c.reduceTasksFinished = make([]bool, nReduce)

	go func() {
		for {
			c.mu.Lock()
			c.cond.Broadcast()
			c.mu.Unlock()
			time.Sleep(time.Second)
		}
	}()
	c.server()
	return &c
}
