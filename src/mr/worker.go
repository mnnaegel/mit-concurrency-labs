package mr

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % BucketCount to choose the reduce
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

func handleMapTask(mapf func(string, string) []KeyValue, reply MapTaskReply) {
	currentDir, err := os.Getwd()
	if err != nil {
		log.Fatalf("cannot get current directory")
	}

	fileToProcess := reply.JobFile
	bucketsToCreate := reply.BucketCount

	contents := getFileContents(fileToProcess)
	intermediateKvpArray := mapf(fileToProcess, contents)
	sort.Sort(ByKey(intermediateKvpArray))

	// Create the temporary files to store results of the map phase
	temporaryIntermediateFiles := make([]*os.File, bucketsToCreate)
	for i := 0; i < len(temporaryIntermediateFiles); i++ {
		temporaryIntermediateFiles[i], err = os.CreateTemp(currentDir+"/tmp", "mr-tmp-"+workerState.WorkerId+"-"+fileToProcess)
		if err != nil {
			log.Fatalf("cannot create temp file")
		}
	}

	// Put KVPs into respective buckets based on hash of the key
	bucketsData := make([][]KeyValue, bucketsToCreate)
	i := 0
	for i < len(intermediateKvpArray) {
		j := i + 1
		for j < len(intermediateKvpArray) && intermediateKvpArray[j].Key == intermediateKvpArray[i].Key {
			j++
		}
		bucketNumber := ihash(intermediateKvpArray[i].Key) % bucketsToCreate
		bucketsData[bucketNumber] = append(bucketsData[bucketNumber], intermediateKvpArray[i:j]...)
		i = j
	}

	// Write JSON intermediate KVPs to temp files
	for i, tempFileContents := range bucketsData {
		enc := json.NewEncoder(temporaryIntermediateFiles[i])
		err := enc.Encode(&tempFileContents)
		if err != nil {
			log.Fatalf("cannot encode json")
		}
		temporaryIntermediateFiles[i].Close()
	}

	// Atomic rename temp files to final files
	for i, tempFile := range temporaryIntermediateFiles {
		// Create the directory if it doesn't already exist
		finalFileDir := filepath.Dir(filepath.Join(currentDir, "intermediate", fileToProcess, strconv.Itoa(i)))
		err := os.MkdirAll(finalFileDir, 0755)
		if err != nil {
			log.Fatalf("cannot create intermediate directory")
		}

		err = os.Rename(tempFile.Name(), currentDir+"/intermediate/"+fileToProcess+"/"+strconv.Itoa(i))
		if err != nil {
			log.Fatalf("Failed renaming temp file %v, error: %v", tempFile.Name(), err)
		}
	}
}

func handleExitTask() {
	os.Exit(0)
}

func handleWaitTask() {
	time.Sleep(time.Second)
}

func handleReduceTask(reducef func(string, []string) string, reply ReduceTaskReply) {
	intermediateKvps := make([]KeyValue, 0)

	currentDir, err := os.Getwd()
	if err != nil {
		log.Fatalf("cannot get current directory")
	}

	intermediateDir := filepath.Join(currentDir, "intermediate")
	desiredFile := strconv.Itoa(reply.BucketNumber)
	err = filepath.Walk(intermediateDir, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() && strings.HasSuffix(path, desiredFile) {
			fmt.Println("Reading KVPs from intermediate file: ", path)
			file, err := os.Open(path)
			if err != nil {
				log.Fatalf("cannot open %v", path)
			}

			dec := json.NewDecoder(file)
			for {
				var kvps []KeyValue
				if err := dec.Decode(&kvps); err != nil {
					break
				}
				intermediateKvps = append(intermediateKvps, kvps...)
			}

			file.Close()
		}
		return nil
	})

	// sort the intermediate kvps
	sort.Sort(ByKey(intermediateKvps))

	// create a temporary file to store the reduce output
	tempFile, err := os.CreateTemp(currentDir+"/tmp", "mr-tmp-"+strconv.Itoa(reply.BucketNumber))
	if err != nil {
		log.Fatalf("cannot create temp file")
	}

	// call Reduce on each distinct key in intermediate[], and print the result to temp file
	i := 0
	for i < len(intermediateKvps) {
		j := i + 1
		for j < len(intermediateKvps) && intermediateKvps[j].Key == intermediateKvps[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediateKvps[k].Value)
		}
		output := reducef(intermediateKvps[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediateKvps[i].Key, output)
		i = j
	}

	tempFile.Close()

	fmt.Println("Writing temporary reduce output to file: ", tempFile.Name())

	// Atomic rename temp file to final file
	err = os.Rename(tempFile.Name(), currentDir+"/mr-out-"+strconv.Itoa(reply.BucketNumber))
	if err != nil {
		log.Fatalf("Failed renaming temp file %v, error: %v", tempFile.Name(), err)
	}

	fmt.Println("Reduce output written to file: ", currentDir+"/mr-out-"+strconv.Itoa(reply.BucketNumber))
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		assignedTask := CallGetTask()

		switch assignedTask.(type) {
		case MapTaskReply:
			fmt.Println("Map task assigned, file: ", assignedTask.(MapTaskReply).JobFile)
			handleMapTask(mapf, assignedTask.(MapTaskReply))
			fmt.Println("Map task completed")
			CallJobFinish(assignedTask.(MapTaskReply).JobId)
		case ReduceTaskReply:
			fmt.Println("Reduce task assigned, bucket: ", assignedTask.(ReduceTaskReply).BucketNumber)
			handleReduceTask(reducef, assignedTask.(ReduceTaskReply))
			fmt.Println("Reduce task completed")
			CallJobFinish(assignedTask.(ReduceTaskReply).JobId)
		case ExitTaskReply:
			fmt.Println("Exit task assigned")
			handleExitTask()
		case WaitTaskReply:
			fmt.Println("Wait task assigned")
			handleWaitTask()
		}
	}
}

func CallJobFinish(jobId string) {
	args := JobFinishArgs{JobId: jobId}
	reply := JobFinishReply{}
	ok := call("Coordinator.JobFinish", &args, &reply)
	if !ok {
		panic("Failed to finish job")
	}
}

func CallGetTask() TaskReply {
	args := GetTaskArgs{WorkerId: workerState.WorkerId}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		panic("Failed to get task")
	}

	switch reply.TaskReply.(type) {
	case MapTaskReply:
		return reply.TaskReply.(MapTaskReply)
	case ReduceTaskReply:
		return reply.TaskReply.(ReduceTaskReply)
	case ExitTaskReply:
		return reply.TaskReply.(ExitTaskReply)
	case WaitTaskReply:
		return reply.TaskReply.(WaitTaskReply)
	default:
		panic("Unknown task type")
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
