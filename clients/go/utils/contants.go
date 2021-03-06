package utils

import "time"

const (
	DefaultRetries        = 3
	RequestTimeoutInSec   = 5
	StreamTimeoutInSec    = 15
	DefaultJobTimeoutInMs = int64(time.Duration(5*time.Minute) / time.Millisecond)
	DefaultJobWorkerName  = "default"
)
