package collector

import "time"

type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func NewSystemClock() Clock {
	return systemClock{}
}

func (systemClock) Now() time.Time {
	return time.Now()
}
