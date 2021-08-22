package mr

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"path"
	"time"
)
const RETRY_TIMES int = 10


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


func Map(mapf func(string,string)[]KeyValue, args WorkInitReply) {
  background := context.Background()
  ctx,cancel:=context.WithCancel(background)
  defer cancel()
  go func() {
    for {
      select {
      case <-ctx.Done() :
        break
      default:
        reply,err:=CallHeartBeat(HeartBeatArgs{Id:args.Id,Type:args.Type})
        if err!=nil || reply.Reset {
          cancel()
          break
        }
      }
    }
  }()
  end:=func()bool{
    select {
    case <-ctx.Done():
      return true
    default:
      return false
    }
  }
  files:=make([]*os.File,args.NReduce)
  var err error
  for id:=range files {
    files[id],err=os.Create(path.Join(args.RootDirectory,fmt.Sprint(id)))
    if err!=nil{
      panic(err)
    }
    defer files[id].Close()
  }
  inputFile,err:=os.Open(args.InputPath[0])
  if err!=nil{
    panic(err)
  }
  defer inputFile.Close()
  contents,err:=ioutil.ReadAll(inputFile)
  if err != nil{
    panic(err)
  }
  kvs:=mapf(inputFile.Name(),string(contents))
  if end() {
    return
  }
  for _,kv:=range kvs{
    hashid:=ihash(kv.Key)%args.NReduce
    files[hashid].WriteString(kv.Key+"\n")
    files[hashid].WriteString(kv.Value+"\n")
  }
  if end() {
    return
  }
  CallWorkFinish(WorkFinishArgs{Id:args.Id,Type:args.Type, DirPath: args.RootDirectory})
}

func Reduce(reducef func(string, []string) string, args WorkInitReply) {
  background := context.Background()
  ctx,cancel:=context.WithCancel(background)
  defer cancel()
  go func() {
    for {
      select {
      case <-ctx.Done() :
        break
      default:
        reply,err:=CallHeartBeat(HeartBeatArgs{Id:args.Id,Type:args.Type})
        if err!=nil || reply.Reset {
          break
        }
      }
    }
  }()
  end:=func()bool{
    select {
    case <-ctx.Done():
      return true
    default:
      return false
    }
  }
  input:=map[string][]string{}
  for _,inputFile := range args.InputPath{
    inputFile=path.Join(inputFile,fmt.Sprint(args.WorkId))
    file,err:=os.Open(inputFile)
    if err!=nil {
      panic(err)
    }
    defer file.Close()
    for !end() {
      var key string
      var value string
      n,err:=fmt.Fscanln(file,&key)
      if n==0||err==io.EOF {
        break
      }
      n,err=fmt.Fscanln(file,&value)
      if n==0||err==io.EOF {
        panic("error in read two lines")
      }
      if _,ok:=input[key];!ok {
        input[key]=make([]string, 0)
      }
      input[key]=append(input[key], value)
    }
    if end() {
      return
    }
  }
  output,err:=os.Create(path.Join(args.RootDirectory,"output"))
  if err!=nil{
    panic(err)
  }
  defer output.Close()
  for k,v:=range input{
    out:=reducef(k,v)
		fmt.Fprintf(output, "%v %v\n", k, out)
  }
  CallWorkFinish(WorkFinishArgs{Id:args.Id,Type:args.Type,DirPath:args.RootDirectory})
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the Coordinator.
	// CallExample()

  for {
    WorkInitReply,err:=CallWorkInit(WorkInitArgs{})
    if err!=nil {
      fmt.Print(err)
      return
    }
    if WorkInitReply.Type == MAP {
      Map(mapf,WorkInitReply)
    } else if WorkInitReply.Type == REDUCE {
      Reduce(reducef,WorkInitReply)
    } else {
      break
    }
  }

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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func CallWorkFinish(args WorkFinishArgs) (WorkFinishReply,error) {
  reply := WorkFinishReply{}
  for i:=0;i<RETRY_TIMES;i++{
    if call("Coordinator.WorkFinish", &args, &reply) {
      return reply,nil
    }
    t:=time.Duration(rand.Int31n(1000))*time.Millisecond
    time.Sleep(t)
  }
  return reply,errors.New("rpc timeout")
}

func CallWorkInit(args WorkInitArgs) (WorkInitReply,error) {
  reply := WorkInitReply{}
  for i:=0;i<RETRY_TIMES;i++{
    if call("Coordinator.WorkInit", &args, &reply) {
      return reply,nil
    }
    t:=time.Duration(rand.Int31n(1000))*time.Millisecond
    time.Sleep(t)
  }
  return reply,errors.New("rpc timeout")
}

func CallHeartBeat(args HeartBeatArgs) (HeartBeatReply,error) {
  reply := HeartBeatReply{}
  for i:=0;i<RETRY_TIMES;i++{
    if call("Coordinator.HeartBeat", &args, &reply) {
      return reply,nil
    }
    t:=time.Duration(rand.Int31n(1000))*time.Millisecond
    time.Sleep(t)
  }
  return reply,errors.New("rpc timeout")
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
