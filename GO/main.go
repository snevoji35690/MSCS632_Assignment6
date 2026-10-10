
package main

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type Task struct {
	ID    int
	Value int
}

type Result struct {
	TaskID int
	Value  int
}

type SharedQueue struct {
	tasks chan Task
}

func NewSharedQueue(size int) *SharedQueue {
	return &SharedQueue{
		tasks: make(chan Task, size),
	}
}

func (q *SharedQueue) addTask(task Task) {
	q.tasks <- task
}

func (q *SharedQueue) getTask() (Task, bool) {
	task, ok := <-q.tasks
	return task, ok
}

func (q *SharedQueue) close() {
	close(q.tasks)
}

func worker(
	id int,
	queue *SharedQueue,
	results chan<- Result,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	log.Printf("Worker %d started.", id)
	defer log.Printf("Worker %d completed.", id)

	for {
		task, ok := queue.getTask()

		if !ok {
			return
		}

		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf(
						"Worker %d error on Task %d: %v",
						id, task.ID, r,
					)
				}
			}()

			time.Sleep(200 * time.Millisecond)

			processed := task.Value * task.Value

			results <- Result{
				TaskID: task.ID,
				Value:  processed,
			}

			log.Printf(
				"Worker %d processed Task %d",
				id, task.ID,
			)
		}()
	}
}

func saveResults(filename string, results []Result) error {
	var lines []string

	for _, result := range results {
		lines = append(
			lines,
			fmt.Sprintf(
				"Task %d -> Result: %d",
				result.TaskID, result.Value,
			),
		)
	}

	return os.WriteFile(
		filename,
		[]byte(strings.Join(lines, "\n")+"\n"),
		0644,
	)
}

func main() {
	log.SetFlags(0)

	const taskCount = 10
	const workerCount = 3

	queue := NewSharedQueue(taskCount)
	results := make(chan Result, taskCount)

	var wg sync.WaitGroup

	for i := 1; i <= workerCount; i++ {
		wg.Add(1)
		go worker(i, queue, results, &wg)
	}

	for i := 1; i <= taskCount; i++ {
		queue.addTask(Task{
			ID:    i,
			Value: i * 10,
		})
	}

	queue.close()

	go func() {
		wg.Wait()
		close(results)
	}()

	var collected []Result

	for result := range results {
		collected = append(collected, result)
	}

	sort.Slice(collected, func(i, j int) bool {
		return collected[i].TaskID < collected[j].TaskID
	})

	if err := saveResults("go_results.txt", collected); err != nil {
		log.Printf("File error: %v", err)
	} else {
		fmt.Println("Results saved to go_results.txt")
	}

	seen := make(map[int]bool)

	for _, result := range collected {
		fmt.Printf(
			"Task %d -> Result: %d\n",
			result.TaskID, result.Value,
		)
		seen[result.TaskID] = true
	}

	fmt.Printf(
		"Processed %d of %d tasks.\n",
		len(collected), taskCount,
	)

	if len(collected) == taskCount &&
		len(seen) == taskCount {
		fmt.Println("All tasks processed exactly once.")
	} else {
		log.Println(
			"Warning: Missing or duplicate task results.",
		)
	}
}
