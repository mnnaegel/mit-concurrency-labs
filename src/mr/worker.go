package mr

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
)
import "log"
import "time"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type WorkerState struct {
	WorkerId string
}

var workerState WorkerState

func init() {
	workerState.WorkerId = newWorkerId()
}

func newWorkerId() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		log.Fatal(err)
	}
	return base64.URLEncoding.EncodeToString(b)
}

func getFileContents(filename string) string {
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	contents, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	return string(contents)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	startTime := time.Now()
	currentDir, err := os.Getwd()
	if err != nil {
		log.Fatalf("cannot get current directory")
	}
	fileToProcess := CallGetTask()

	contents := getFileContents(fileToProcess)
	intermediateKvpArray := mapf(fileToProcess, contents)

	// sort by keys
	sort.Sort(ByKey(intermediateKvpArray))

	// create 10 temporary files with os.CreateTemp
	temporaryIntermediateFiles := make([]*os.File, 10)
	for i := 0; i < 10; i++ {
		temporaryIntermediateFiles[i], err = os.CreateTemp(currentDir+"/tmp", "mr-tmp-"+workerState.WorkerId+"-"+fileToProcess)
		if err != nil {
			log.Fatalf("cannot create temp file")
		}
	}

	// bucket the kvps
	bucketsData := make([][]KeyValue, 10)
	i := 0
	for i < len(intermediateKvpArray) {
		j := i + 1
		for j < len(intermediateKvpArray) && intermediateKvpArray[j].Key == intermediateKvpArray[i].Key {
			j++
		}
		bucketNumber := ihash(intermediateKvpArray[i].Key) % 10
		bucketsData[bucketNumber] = append(bucketsData[bucketNumber], intermediateKvpArray[i:j]...)
		i = j
	}

	fmt.Println("Finished grouping KVPs into buckets: ", time.Since(startTime))

	// write to temp files
	for i, tempFileContents := range bucketsData {
		// write to temp file json of kvps
		enc := json.NewEncoder(temporaryIntermediateFiles[i])
		err := enc.Encode(&tempFileContents)
		if err != nil {
			log.Fatalf("cannot encode json")
		}
		temporaryIntermediateFiles[i].Close()
	}

	fmt.Println("Finished writing temp files: ", time.Since(startTime))

	for i, tempFile := range temporaryIntermediateFiles {
		err := os.Rename(tempFile.Name(), currentDir+"/mr-out-"+fileToProcess+"-"+strconv.Itoa(i))
		if err != nil {
			log.Fatalf("Failed renaming temp file %v, error: %v", tempFile.Name(), err)
		}
	}

	fmt.Println("Total time: ", time.Since(startTime))
}

func CallGetTask() string {
	args := GetTaskArgs{WorkerId: workerState.WorkerId}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		fmt.Printf("reply.TaskFile %v\n", reply.TaskFile)
	} else {
		fmt.Printf("call failed!\n")
	}

	return reply.TaskFile
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
