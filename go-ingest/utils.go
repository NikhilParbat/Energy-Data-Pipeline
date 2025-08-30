package main

import (
	"log"
	"time"
)

func ParseTime(ts string) time.Time {
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		log.Fatalf("Failed to parse time %s: %v", ts, err)
	}
	return t
}

func Retry(attempts int, sleep time.Duration, fn func() error) error {
	for i := range attempts {
		if err := fn(); err != nil {
			if i < attempts-1 {
				time.Sleep(sleep)
				continue
			} else {
				return err
			}
		}
		return nil
	}
	return nil
}
