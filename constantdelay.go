package cron

import "time"

// ConstantDelaySchedule represents a simple recurring duty cycle, e.g. "Every 5 minutes".
// It does not support jobs more frequent than once a second.
type ConstantDelaySchedule struct {
	Delay time.Duration
}

// Every returns a crontab Schedule that activates once every duration.
// Delays of less than a second are not supported (will round up to 1 second).
// Any fields less than a Second are truncated.
func Every(duration time.Duration) ConstantDelaySchedule {
	if duration < time.Second {
		duration = time.Second
	}
	return ConstantDelaySchedule{
		Delay: duration - time.Duration(duration.Nanoseconds())%time.Second,
	}
}

// Next returns the next time this should be run.
// This rounds so that the next activation time will be on the second.
// If `after` time is specified then the next activation time later than
// `after` is calculated.
func (schedule ConstantDelaySchedule) Next(from, after time.Time) time.Time {
	if after.IsZero() || after.Before(from) {
		return from.Add(schedule.Delay - time.Duration(from.Nanosecond())*time.Nanosecond)
	} else {
		diff := after.Sub(from) + time.Duration(from.Nanosecond())*time.Nanosecond
		timeDelay := (diff / schedule.Delay) * schedule.Delay
		return from.Add(timeDelay).Add(schedule.Delay - time.Duration(from.Nanosecond())*time.Nanosecond)
	}
}
