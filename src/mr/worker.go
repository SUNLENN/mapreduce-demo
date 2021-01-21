package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
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


// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

	for {

		replay := CallGetTask()
		// map task
		if replay.TaskType == 1 {
			filename := replay.Input[0]
			intermediate := []KeyValue{}

			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}

			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}

			file.Close()
			kva := mapf(filename, string(content))
			intermediate = append(intermediate, kva...)

			outputs := make([]*os.File, replay.R)

			for i := 0; i < replay.R; i += 1 {
				tmpFile, err := ioutil.TempFile(".", "tmp-")
				if err != nil {
					log.Fatalf("cannot create temporary file %v", tmpFile.Name())
				}
				outputs[i] = tmpFile
			}

			for _, kv := range intermediate {
				i := ihash(kv.Key) % replay.R
				enc := json.NewEncoder(outputs[i])
				err = enc.Encode(&kv)
				if err != nil {
					log.Fatalf("Failed to encode the keyValue: %v", err)
				}
			}


			for i, fp := range outputs {
				//fmt.Println(replay.TaskId)
				//fmt.Println(i)
				name := "mr-" + strconv.Itoa(replay.TaskId) + "-" + strconv.Itoa(i)
				err = os.Rename(fp.Name(), name)

				if err != nil {
					//fmt.Println(fp.Name() + " " + name)
					log.Fatalf("Failed to rename the temp file %v", err)
				}
				err = fp.Close()
				if err != nil {
					log.Fatalf("Failed to close the file %v", fp.Name())
				}
			}

			CallCompleteTask(replay.TaskType, replay.TaskId)

		} else if replay.TaskType == 2 {
			intermediate := []KeyValue{}
			for _, filename := range replay.Input {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						//log.Fatal("Failed to decode the keyValue", err)
						break
					}
					intermediate = append(intermediate, kv)
				}
				err = file.Close()
				if err != nil {
					log.Fatalf("Failed to close the file %v", file.Name())
				}

			}


			sort.Sort(ByKey(intermediate))


			tmpFile, err := ioutil.TempFile(".", "tmp-")
			if err != nil {
				log.Fatalf("Cannot create temporary file: %v", err)
			}


			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				_, err = fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

				if err != nil {

				}

				i = j
			}

			oname := "mr-out-" + strconv.Itoa(replay.TaskId)

			err = os.Rename(tmpFile.Name(), oname)

			if err != nil {
				log.Fatal("Failed to rename the temp file", err)
			}

			err = tmpFile.Close()
			if err != nil {
				log.Fatal("Failed to close the temp file", err)
			}
			CallCompleteTask(replay.TaskType, replay.TaskId)

		}
		// wait for a second
		time.Sleep(time.Second)
	}



}


func CallGetTask() GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	call("Master.GetTask", &args, &reply)
	return reply
}

func CallCompleteTask(taskType int, taskId int) {
	args := CompleteTaskArgs{}
	reply := CompleteTaskReply{}

	args.TaskId = taskId
	args.TaskType = taskType

	call("Master.CompleteTask", &args, &reply)
}
//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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

