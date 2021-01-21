package mr

import (
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	// Is this Map/Reduce idle ?
	// 0 : idle
	// 1 : not completed
	// 2 : completed
	IsIdleMaps []int
	IsIdleReduces []int

	// file name for tasks
	MapTasks []string
	ReduceTasks [][]string

	MapTasksTime []int64
	ReduceTasksTime []int64

	// M map task
	M int
	// R reduce task
	R int

	// lock
	Mux sync.Mutex
}

const TIMEOUT = 10

// Worker use this rpc to ask for a task from the master
func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {

	if !m.Done() {
		//fmt.Println(m.isAllMapCompleted())
		if !m.isAllMapCompleted() {
			m.Mux.Lock()
			for i := 0; i < m.M; i += 1 {
				if m.IsIdleMaps[i] == 0 {
					m.IsIdleMaps[i] = 1
					reply.TaskId = i
					reply.TaskType = 1
					input := []string{m.MapTasks[i]}
					reply.Input = input
					reply.R = m.R
					m.MapTasksTime[i] = time.Now().Unix()
					break
				}
			}
			m.Mux.Unlock()
		} else {
			m.Mux.Lock()
			for i := 0; i < m.R; i += 1 {
				if m.IsIdleReduces[i] == 0 {
					m.IsIdleReduces[i] = 1
					reply.TaskId = i
					reply.TaskType = 2
					reply.Input = m.ReduceTasks[i]
					m.ReduceTasksTime[i] = time.Now().Unix()
					break
				}
			}
			m.Mux.Unlock()
		}
	}
	return nil
}

func (m *Master) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	//fmt.Printf("%v\n", m.IsIdleMaps)
	if args.TaskType == 1 {
		m.Mux.Lock()
		m.IsIdleMaps[args.TaskId] = 2
		m.Mux.Unlock()
	} else {
		m.Mux.Lock()
		m.IsIdleReduces[args.TaskId] = 2
		m.Mux.Unlock()
	}
	return nil
}
//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {

	if !m.isAllMapCompleted() {
		m.Mux.Lock()
		for i := 0; i < m.M; i += 1 {
			if m.IsIdleMaps[i] == 1 {
				if time.Now().Unix() - m.MapTasksTime[i] > TIMEOUT {
					m.IsIdleMaps[i] = 0
				}
			}
		}
		m.Mux.Unlock()
	} else {
		m.Mux.Lock()
		for i := 0; i < m.R; i += 1 {
			if m.IsIdleReduces[i] == 1 {
				if time.Now().Unix() - m.ReduceTasksTime[i] > TIMEOUT {
					m.IsIdleReduces[i] = 0
				}
			}
		}
		m.Mux.Unlock()
	}

	for i := 0; i < m.R; i += 1 {
		if m.IsIdleReduces[i] != 2 {
			return false
		}
	}

	return true
}

func (m *Master) isAllMapCompleted() bool {
	for i := 0; i < m.M; i+= 1 {
		if m.IsIdleMaps[i] != 2 {
			return false
		}
	}
	return true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.M = len(files)
	m.R = nReduce

	m.MapTasks = files
	m.IsIdleMaps = make([]int, m.M)

	m.ReduceTasks = make([][]string, m.R)
	m.IsIdleReduces = make([]int, m.R)


	m.MapTasksTime = make([]int64, m.M)
	m.ReduceTasksTime = make([]int64, m.R)

	for i := 0; i < m.R; i += 1 {
		m.ReduceTasks[i] = make([]string, m.M)
		for j := 0; j < m.M; j += 1 {
			name := "mr-" + strconv.Itoa(j) + "-" + strconv.Itoa(i)
			m.ReduceTasks[i][j] = name
		}
	}


	// Your code here.
	m.server()
	return &m
}
