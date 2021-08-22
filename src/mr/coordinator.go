package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"sync"
	"time"
)



type WorkStatus int;

const (
  WAITTING WorkStatus = iota
  RUNNING
  DONE
)

const (
  MAP int = iota
  REDUCE
  END
)

const LockTimeout time.Duration = time.Second*3
const HeartBeatTimeout time.Duration = time.Second*10

type Work struct {
  WorkId int
  Status WorkStatus
  InputPath *[]string
  Type int
  ResultDirectory string
}

type WorkTimer struct {
  *time.Timer
  WorkId int
}

type Coordinator struct {
	// Your definitions here.
  NReduce int
  NMap int
  Status int
  WorkToDo chan *Work
  ReduceInput []string
  Files []string
  Works []*Work
  Timers []*WorkTimer
  FinishedWorkCount int
  lock sync.Locker
  RootDirectory string
}

func (m *Coordinator)GetLockWithTimeout(timeout time.Duration) error {
  end:=false
  timer:=time.AfterFunc(timeout,func() { end=true })
  m.lock.Lock()
  if end {
    m.lock.Unlock()
    return errors.New("timeout")
  } else {
    timer.Stop()
    return nil
  }
}

func (m *Coordinator) InitMapWork() bool {
  m.Works = make([]*Work, m.NMap)
  m.Status=MAP
  m.Timers=make([]*WorkTimer, 0)
  m.WorkToDo=make(chan *Work, m.NMap)
  for id:=range(m.Works) {
    m.Works[id]=&Work{
      Status:WAITTING,
      InputPath: &[]string{m.Files[id]},
      Type: MAP,
      WorkId: id,
    }
    m.WorkToDo<-m.Works[id]
  }
  m.FinishedWorkCount=0
  return true
}
func (m *Coordinator) InitReduceWork() bool {
  if m.Status!=MAP || m.FinishedWorkCount != m.NMap{
    return false
  }
  m.Status=REDUCE
  m.Timers=make([]*WorkTimer, 0)
  m.ReduceInput = make([]string, m.NMap)
  m.WorkToDo=make(chan *Work, m.NReduce)
  for id,work:=range(m.Works) {
    m.ReduceInput[id]=work.ResultDirectory
  }
  m.Works = make([]*Work, m.NReduce)
  for id:=range(m.Works) {
    m.Works[id]=&Work{
      Status:WAITTING,
      InputPath: &m.ReduceInput,
      Type: REDUCE,
      WorkId: id,
    }
    m.WorkToDo<-m.Works[id]
  }
  m.FinishedWorkCount=0
  return true
}

func (m *Coordinator) Ended() bool {
  if m.Status==MAP {
    return false
  }
  if m.Status==REDUCE {
    if m.FinishedWorkCount==m.NReduce {
      m.Status=END
      m.FinishedWorkCount=0
    }
  }
  return m.Status==END
}

func (m *Coordinator)GetWork() *Work {
  if m.Ended() {
    return nil
  }
  m.InitReduceWork()
  for len(m.WorkToDo) != 0 {
    ret:=<-m.WorkToDo
    if ret.Status != DONE {
      m.WorkToDo<-ret
      return ret
    }
  }
  panic("failed to find work")
}

// Your code here -- RPC handlers for the worker to call.
func (m *Coordinator)HeartBeat(args *HeartBeatArgs, reply *HeartBeatReply) error {
  if err:=m.GetLockWithTimeout(LockTimeout); err!=nil{
    return err
  }
  defer m.lock.Unlock()
  if args.Id>=len(m.Timers) || !m.Timers[args.Id].Reset(HeartBeatTimeout) || m.Works[m.Timers[args.Id].WorkId].Status == DONE {
    reply.Reset=true
  }
  return nil
}

func (m *Coordinator)WorkInit(args *WorkInitArgs, reply *WorkInitReply) error {
  if err:=m.GetLockWithTimeout(LockTimeout); err!=nil{
    return err
  }
  defer m.lock.Unlock()
  work:=m.GetWork()
  if work == nil{
    return errors.New("all work finished")
  }
  reply.Type=work.Type
  m.Timers = append(m.Timers, &WorkTimer{Timer: time.NewTimer(HeartBeatTimeout),WorkId: work.WorkId})
  reply.Id=len(m.Timers)-1
  reply.NReduce=m.NReduce
  reply.NMap=m.NMap
  reply.InputPath=*work.InputPath
  reply.WorkId=work.WorkId
  if work.Type == MAP {
    reply.RootDirectory=path.Join(m.RootDirectory,"map",fmt.Sprint(reply.Id))
  } else if work.Type == REDUCE {
    reply.RootDirectory=path.Join(m.RootDirectory,"reduce",fmt.Sprint(reply.Id))
  }
  err:=os.MkdirAll(reply.RootDirectory,os.ModePerm)
  return err
}

func (m *Coordinator)WorkFinish(args *WorkFinishArgs, reply *WorkFinishReply) error {
  if err:=m.GetLockWithTimeout(LockTimeout); err!=nil{
    return err
  }
  defer m.lock.Unlock()
  if args.Type!=m.Status{
    return nil
  }
  workId:=m.Timers[args.Id].WorkId
  if m.Works[workId].Status == DONE {
    return nil
  }
  m.Works[workId].Status = DONE
  m.FinishedWorkCount++
  m.Works[workId].ResultDirectory=args.DirPath
  return nil
}


// Your code here -- RPC handlers for the worker to call.

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
func (m *Coordinator) server() {
	rpc.Register(m)
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
// main/mrCoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Coordinator) Done() bool {
  if err:=m.GetLockWithTimeout(LockTimeout); err!=nil{
    return false
  }
  defer m.lock.Unlock()

	ret := (m.Status==END)

  if _,err:=os.Stat(path.Join(m.RootDirectory,fmt.Sprintf("mr-out-%d",m.NReduce-1))); ret&&err!=nil {
    for id,work:=range m.Works {
      os.Rename(path.Join(work.ResultDirectory,"output"),path.Join(m.RootDirectory,fmt.Sprintf("mr-out-%d",id)))
    }
  }

	return ret
}

//
// create a Coordinator.
// main/mrCoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
  rootDirectory:="."
	m := Coordinator{
    NReduce: nReduce,
    NMap: len(files),
    Status: MAP,
    Files: files,
    lock: &sync.Mutex{},
    RootDirectory: rootDirectory,
	}
  m.InitMapWork()

	// Your code here.

	m.server()
	return &m
}
