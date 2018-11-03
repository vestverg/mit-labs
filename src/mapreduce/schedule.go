package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	gr := sync.WaitGroup{}
	gr.Add(ntasks)
	for i := 0; i < ntasks; i++ {
		j := i
		job := NewJob(mapFiles[i], jobName, j)
		go submitJob(job, registerChan, phase, n_other, &gr)
	}
	gr.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}

func NewJob(file string, jobName string, number int) Job {
	return Job{
		name:   jobName,
		number: number,
		file:   file,
	}
}

func submitJob(job Job, workers chan string, phase jobPhase, n_other int, wg *sync.WaitGroup) {
	doTaskArgs := DoTaskArgs{
		JobName:       job.name,
		File:          job.file,
		Phase:         phase,
		TaskNumber:    job.number,
		NumOtherPhase: n_other,
	}
	defer wg.Done()
	for {
		worker := <-workers
		if call(worker, "Worker.DoTask", doTaskArgs, new(struct{})) {
			go func(w string) { workers <- w }(worker)
			return
		}
	}
}

type Job struct {
	name   string
	number int
	file   string
}
