package workers

import (
	"fmt"
	"runtime"
	"time"
)

type MiddlewareLogging struct{}

func (l *MiddlewareLogging) Call(queue string, message *Msg, next func() bool) (acknowledge bool) {
	prefix := fmt.Sprint(queue, " JID-", message.Jid())

	start := time.Now()
	Logger.Println(prefix, "start")
	Logger.Println(prefix, "args: ", message.Args().ToJson())

	defer func() {
		// do not log retry panics. They are raised deliberately
		if e := recover(); e != nil {
			if _, ok := e.(ErrDoRetry); !ok {
				Logger.Println(prefix, "fail:", time.Since(start))

				buf := make([]byte, 4096)
				buf = buf[:runtime.Stack(buf, false)]
				Logger.Printf("%s error: %v\n%s", prefix, e, buf)
			}
			panic(e)
		}
	}()

	acknowledge = next()

	Logger.Println(prefix, "done:", time.Since(start))

	return
}
