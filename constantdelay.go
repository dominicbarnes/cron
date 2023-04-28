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
// t is the time instant to calculate the next instant from.
func (schedule ConstantDelaySchedule) Next(t time.Time) time.Time {
	return schedule.NextWithAfter(t, time.Time{})
}

// NextWithAfter returns the next time this should be run which is later than the provided `after`.
// This method calculates the next activation time by adding the delay to the `from` time param.
// If `after` time specified is non-zero and later than `from`, then the diff between them is
// calculated. This time difference is divided by the schedule delay interval to round off to the
// nearest multiple that should be added to `from` to bring it closest to `after`. The delay interval
// is added once more to get the new time which will be guaranteed to be later than `after`.
// Note: additional nanoseconds are rounded off to the nearest second.
//
// `from` is the starting time instant to calculate the delay from.
// `after` is the time instant the new calculated next should be later than.
// TFROM + delay*M = TNEXT such that TNEXT  is after TAFTER
func (schedule ConstantDelaySchedule) NextWithAfter(from, after time.Time) time.Time {
	if after.IsZero() || after.Before(from) {
		return from.Add(schedule.Delay - time.Duration(from.Nanosecond())*time.Nanosecond)
	} else {
		// calculate total delay between start and from
		diff := after.Sub(from) + time.Duration(from.Nanosecond())*time.Nanosecond
		// find the lowest multiple of delay that needs to be added to from
		timeDelay := (diff / schedule.Delay) * schedule.Delay
		// add another round delay and round off nanoseconds to get the nearest second
		return from.Add(timeDelay).Add(schedule.Delay - time.Duration(from.Nanosecond())*time.Nanosecond)
	}
}
