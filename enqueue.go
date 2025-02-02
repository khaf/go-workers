package workers

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

type EnqueueData struct {
	Queue      string      `json:"queue,omitempty"`
	Class      string      `json:"class"`
	Args       interface{} `json:"args"`
	Jid        string      `json:"jid"`
	EnqueuedAt float64     `json:"enqueued_at"`
	EnqueueOptions
}

type EnqueueOptions struct {
	RetryCount int     `json:"retry_count,omitempty"`
	Retry      bool    `json:"retry,omitempty"`
	At         float64 `json:"at,omitempty"`
}

func generateJid() string {
	// Return 12 random bytes as 24 character hex
	b := make([]byte, 12)
	_, err := io.ReadFull(rand.Reader, b)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%x", b)
}

func Enqueue(queue, class string, args interface{}) (string, error) {
	return EnqueueWithOptions(queue, class, args, EnqueueOptions{})
}

func EnqueueIn(queue, class string, in time.Duration, args interface{}) (string, error) {
	return EnqueueWithOptions(queue, class, args, EnqueueOptions{At: ToNano(time.Now().Add(in))})
}

func EnqueueAt(queue, class string, at time.Time, args interface{}) (string, error) {
	return EnqueueWithOptions(queue, class, args, EnqueueOptions{At: ToNano(at)})
}

func EnqueueWithOptions(queue, class string, args interface{}, opts EnqueueOptions) (string, error) {
	data := EnqueueData{
		Queue:          queue,
		Class:          class,
		Args:           args,
		Jid:            generateJid(),
		EnqueuedAt:     float64(time.Now().UnixNano()) / 1000000000,
		EnqueueOptions: opts,
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		return "", err
	}

	if nowNano() < opts.At {
		err := enqueueAt(data.At, bytes)
		return data.Jid, err
	}

	conn := Config.Pool.Get()
	defer conn.Close()

	_, err = conn.Do("sadd", Config.Namespace+"queues", queue)
	if err != nil {
		return "", err
	}
	queue = Config.Namespace + "queue:" + queue
	_, err = conn.Do("rpush", queue, bytes)
	if err != nil {
		return "", err
	}

	return data.Jid, nil
}

func enqueueAt(at float64, bytes []byte) error {
	conn := Config.Pool.Get()
	defer conn.Close()

	_, err := conn.Do(
		"zadd",
		Config.Namespace+SCHEDULED_JOBS_KEY, at, bytes,
	)
	if err != nil {
		return err
	}

	return nil
}

func ToNano(t time.Time) float64 {
	return float64(t.UnixNano()) / 1000000
}

func nowNano() float64 {
	return ToNano(time.Now())
}
