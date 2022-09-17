package timeutil

import (
	"runtime"
	"time"
)

func TimerReset(t *time.Timer, d time.Duration) {
	TimerStop(t)
	t.Reset(d)
}

func TimerStop(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
		runtime.Gosched()
	}
}
