package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"sort"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type FileNamePair struct {
	old_filename string
	new_filename string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func rename(filename_pair []FileNamePair) {
	for _, pair := range filename_pair {
		err := os.Rename(pair.old_filename, pair.new_filename)
		if err != nil {
			os.Remove(pair.old_filename)
		}
	}
}

func map_impl(TaskId int, MapFileName string, ReduceNum int, mapf func(string, string) []KeyValue) (bool, []FileNamePair) {
	files := make(map[int]*json.Encoder)
	intermediate := []KeyValue{}
	filename_pair := []FileNamePair{}

	file, err := os.Open(MapFileName)
	if err != nil {
		return false, filename_pair
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return false, filename_pair
	}
	file.Close()
	kva := mapf(MapFileName, string(content))
	intermediate = append(intermediate, kva...)
	for _, pair := range intermediate {
		reduce_id := ihash(pair.Key) % ReduceNum
		encoder, exist := files[reduce_id]
		if exist == false {
			inter_file_name := "mr-" + strconv.Itoa(TaskId) + "-" + strconv.Itoa(reduce_id)
			file, err := os.CreateTemp("", inter_file_name)
			if err != nil {
				return false, filename_pair
			}
			filename_pair = append(filename_pair, FileNamePair{file.Name(), inter_file_name})
			enc := json.NewEncoder(file)
			files[reduce_id] = enc
			encoder = enc
		}
		err = encoder.Encode(&pair)
		if err != nil {
			return false, filename_pair
		}
	}

	defer rename(filename_pair)
	return true, filename_pair
}

func GetTargetFile(pathname string, s []string, id int) ([]string, error) {
	rd, err := ioutil.ReadDir(pathname)
	if err != nil {
		fmt.Println("read dir fail:", err)
		return s, err
	}

	re := "mr-" + ".*?" + "-" + strconv.Itoa(id)
	for _, fi := range rd {
		if !fi.IsDir() {
			match, _ := regexp.MatchString(re, fi.Name())
			if match == true {
				s = append(s, pathname+"/"+fi.Name())
			}
		}
	}
	return s, nil
}

func reduce_impl(TaskId int, reducef func(string, []string) string) (bool, []FileNamePair) {
	filename_pair := []FileNamePair{}
	filenames, err := GetTargetFile("/home/naruto/StudyDir/6.5840/src/main/mr-tmp", []string{}, TaskId)
	if err != nil {
		return false, filename_pair
	}
	intermediate := []KeyValue{}

	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			return false, filename_pair
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(TaskId)
	ofile, err := os.CreateTemp("", oname)
	if err != nil {
		fmt.Println("mr-out* create filed:", err)
	}
	filename_pair = append(filename_pair, FileNamePair{ofile.Name(), oname})

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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	defer rename(filename_pair)
	ofile.Close()
	return true, filename_pair
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		ReqTaskArgs := RequestTaskArgs{}
		ReqTaskReply := RequestTaskReply{}
		ok := call("Coordinator.RequestTask", &ReqTaskArgs, &ReqTaskReply)
		if ok == false {
			break
		}

		if ReqTaskReply.IsSuccess == true {
			if ReqTaskReply.IsMapTask == true {
				ok, filename_pairs := map_impl(ReqTaskReply.TaskId, ReqTaskReply.MapInputFileName, ReqTaskReply.ReduceNum, mapf)
				if ok == true {
					TaskCompletedArgs := MapTaskCompletedArgs{}
					TaskCompletedReply := MapTaskCompletedReply{}
					TaskCompletedArgs.MapTaskId = ReqTaskReply.TaskId
					ok = call("Coordinator.MapTaskCompleted", &TaskCompletedArgs, &TaskCompletedReply)
					if ok == false {
						break
					}
				} else {
					for _, pair := range filename_pairs {
						os.Remove(pair.old_filename)
					}
				}
			} else {
				ok, filename_pairs := reduce_impl(ReqTaskReply.TaskId, reducef)
				if ok == true {
					TaskCompletedArgs := ReduceTaskCompletedArgs{}
					TaskCompletedReply := ReduceTaskCompletedReply{}
					TaskCompletedArgs.ReduceTaskId = ReqTaskReply.TaskId
					ok = call("Coordinator.ReduceTaskCompleted", &TaskCompletedArgs, &TaskCompletedReply)
					if ok == false {
						break
					}
				} else {
					for _, pair := range filename_pairs {
						os.Remove(pair.old_filename)
					}
				}
			}
		}
		// time.Sleep(time.Second)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
