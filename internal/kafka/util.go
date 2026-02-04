package kafka

import (
	"context"
	"errors"
	"log"
	"math"
	"time"
)

func Retry(ctx context.Context, f func() error, retryTime int, noRetryErrs ...error) error {
	err := f()
	if isNoRetryErr(err, noRetryErrs) {
		return err
	}
	for attempt := 0; attempt < retryTime; attempt++ {
		// 指数退避
		delay := time.Duration(math.Pow(2, float64(attempt))) * initialRetryDelay
		delay = min(delay, maxRetryDelay) // 最大退避5s

		log.Printf("retrying, retry time: %d\n", attempt+1)

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
		err = f()

		if isNoRetryErr(err, noRetryErrs) {
			return err
		}
	}

	return err
}

func isNoRetryErr(err error, noRetryErrs []error) bool {
	if err == nil { // err为nil，也不需要重试，直接返回即可
		return true
	}
	for _, target := range noRetryErrs {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}
