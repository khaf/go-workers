package main

import (
	"fmt"
	"time"

	"github.com/khaf/go-workers"
)

func main() {
	workers.Configure(map[string]string{
		"server":    "localhost:6379",
		"database":  "0",
		"pool":      "30",
		"process":   "1",
		"namespace": "goworkers",
	})

	workers.POLL_INTERVAL = 1

	workers.DelayFunction = delay

	workers.Process("myqueue", Task, 10)
	go workers.Run()
	go timer()
	workers.Enqueue("myqueue", "Task", map[string]interface{}{"foo": "bar"})
	workers.EnqueueIn("myqueue", "Task", 5*time.Second, map[string]interface{}{"foo10": "bar10"})
	workers.EnqueueAt("myqueue", "Task", time.Now().Add(19*time.Second), map[string]interface{}{"foo19": "bar19"})
	workers.EnqueueWithOptions("myqueue", "Task", map[string]interface{}{"foo20": "bar20"},
		workers.EnqueueOptions{At: workers.ToNano(time.Now().Add(6 * time.Second)), Retry: true, RetryCount: 10})

	time.Sleep(20 * time.Second)
}

var i = -1

func Task(msg *workers.Msg) {
	i++
	if i != 2 {
		fmt.Println(msg)
	} else {
		panic("failed!")
	}
}

func timer() {
	secs := 0
	for {
		select {
		case <-time.After(time.Second):
			secs++
			workers.Logger.Printf(">> Seconds Elapsed: %d", secs)
		}
	}
}

func delay(count int) time.Duration {
	return 5 * time.Second
}
