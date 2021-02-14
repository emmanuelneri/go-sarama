package test

import (
	"log"
	"time"
)

func WaitForCondition(waitFor string, interval time.Duration, interations int, check func() bool) {
	for i := 0; i < interations; i++ {
		if !check() {
			log.Printf("wait for %s. iteration: %d", waitFor, i)
			time.Sleep(interval)
			continue
		}

		return
	}
}
