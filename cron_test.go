package cron

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Many tests schedule a job for every second, and then wait at most a second
// for it to run.  This amount is just slightly larger than 1 second to
// compensate for a few milliseconds of runtime.
const oneSecond = 1*time.Second + 50*time.Millisecond

type syncWriter struct {
	wr bytes.Buffer
	m  sync.Mutex
}

func (sw *syncWriter) Write(data []byte) (n int, err error) {
	sw.m.Lock()
	n, err = sw.wr.Write(data)
	sw.m.Unlock()
	return
}

func (sw *syncWriter) String() string {
	sw.m.Lock()
	defer sw.m.Unlock()
	return sw.wr.String()
}

func newBufLogger(sw *syncWriter) Logger {
	return PrintfLogger(log.New(sw, "", log.LstdFlags))
}

func TestFuncPanicRecovery(t *testing.T) {
	var buf syncWriter
	cron := New(WithParser(secondParser),
		WithChain(Recover(newBufLogger(&buf))))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cron.Start(ctx)
	cron.AddFunc("* * * * * ?", func(context.Context) {
		panic("YOLO")
	})

	select {
	case <-time.After(oneSecond):
		if !strings.Contains(buf.String(), "YOLO") {
			t.Error("expected a panic to be logged, got none")
		}
		return
	}
}

type PanicJob struct{}

func (d PanicJob) Run(context.Context) { panic("YOLO") }

func TestJobPanicRecovery(t *testing.T) {
	var job PanicJob

	var buf syncWriter
	cron := New(WithParser(secondParser),
		WithChain(Recover(newBufLogger(&buf))))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cron.Start(ctx)

	cron.AddJob("* * * * * ?", job)

	select {
	case <-time.After(oneSecond):
		if !strings.Contains(buf.String(), "YOLO") {
			t.Error("expected a panic to be logged, got none")
		}
		return
	}
}

func TestAddWhenNotRunning(t *testing.T) {
	cron, _, cancel := newWithSeconds()
	defer cancel()

	cron.AddFunc("* * * * * ?", func(context.Context) {
		t.Fatal("should never run")
	})

	<-time.After(oneSecond)
}

// Add a job, start cron, expect it runs.
func TestAddFuncBeforeRunning(t *testing.T) {
	done := make(chan struct{})

	cron, ctx, cancel := newWithSeconds()
	defer cancel()
	cron.AddFunc("* * * * * ?", func(context.Context) { close(done) })
	cron.Start(ctx)

	select {
	case <-time.After(oneSecond):
		t.Fatal("job did not run within 1 second")
	case <-done:
	}
}

// Start cron, add a job, expect it runs.
func TestAddWhileRunning(t *testing.T) {
	done := make(chan struct{})

	cron, ctx, cancel := newWithSeconds()
	defer cancel()
	cron.Start(ctx)

	cron.AddFunc("* * * * * ?", func(context.Context) { close(done) })

	select {
	case <-time.After(oneSecond):
		t.Fatal("job did not run within 1 second")
	case <-done:
	}
}

// Test for #34. Adding a job after calling start results in multiple job invocations
func TestAddWhileRunningWithDelay(t *testing.T) {
	cron, ctx, cancel := newWithSeconds()
	defer cancel()
	cron.Start(ctx)

	time.Sleep(3 * time.Second)

	var calls int64
	cron.AddFunc("* * * * * *", func(context.Context) { atomic.AddInt64(&calls, 1) })

	<-time.After(oneSecond)

	if atomic.LoadInt64(&calls) != 1 {
		t.Errorf("called %d times, expected 1\n", calls)
	}
}

// Add a job, remove a job, start cron, expect nothing runs.
func TestRemoveBeforeRunning(t *testing.T) {
	done := make(chan struct{})

	cron, ctx, cancel := newWithSeconds()
	defer cancel()
	id, _ := cron.AddFunc("* * * * * ?", func(context.Context) { close(done) })
	cron.Remove(id)
	cron.Start(ctx)

	select {
	case <-time.After(oneSecond):
		// Success, shouldn't run
	case <-done:
		t.Errorf("job ran even though it was removed before running")
	}
}

// Start cron, add a job, remove it, expect it doesn't run.
func TestRemoveWhileRunning(t *testing.T) {
	done := make(chan struct{})

	cron, ctx, cancel := newWithSeconds()
	defer cancel()
	cron.Start(ctx)

	id, _ := cron.AddFunc("* * * * * ?", func(context.Context) { close(done) })
	cron.Remove(id)

	select {
	case <-time.After(oneSecond):
	case <-done:
		t.Errorf("job ran even though it was removed immediately")
	}
}

func TestContextPropagation(t *testing.T) {
	cron, ctx, cancel := newWithSeconds()

	done := make(chan struct{})
	cron.AddFunc("* * * * * ?", func(ctx context.Context) {
		<-ctx.Done()
		close(done)
	})

	cron.Start(ctx)

	<-time.After(oneSecond)
	cancel()

	select {
	case <-time.After(oneSecond):
		t.Fatal("context was not propagated to job")
	case <-done:
	}
}

// Test timing with Entries.
func TestSnapshotEntries(t *testing.T) {
	done := make(chan struct{})

	cron := New()
	cron.AddFunc("@every 2s", func(context.Context) { close(done) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cron.Start(ctx)

	select {
	case <-time.After(oneSecond):
		cron.Entries()
	}

	// Even though Entries was called, the cron should fire at the 2 second mark.
	select {
	case <-time.After(oneSecond):
		t.Error("expected job runs at 2 second mark")
	case <-done:
	}
}

// Test that the entries are correctly sorted.
// Add a bunch of long-in-the-future entries, and an immediate entry, and ensure
// that the immediate entry runs immediately.
// Also: Test that multiple jobs run in the same instant.
func TestMultipleEntries(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron, ctx, cancel := newWithSeconds()
	defer cancel()

	cron.AddFunc("0 0 0 1 1 ?", func(context.Context) {})
	cron.AddFunc("* * * * * ?", func(context.Context) { wg.Done() })
	id1, _ := cron.AddFunc("* * * * * ?", func(context.Context) { t.Fatal() })
	id2, _ := cron.AddFunc("* * * * * ?", func(context.Context) { t.Fatal() })
	cron.AddFunc("0 0 0 31 12 ?", func(context.Context) {})
	cron.AddFunc("* * * * * ?", func(context.Context) { wg.Done() })

	cron.Remove(id1)
	cron.Start(ctx)
	cron.Remove(id2)

	select {
	case <-time.After(oneSecond):
		t.Error("expected job run in proper order")
	case <-wait(wg):
	}
}

// Test running the same job twice.
func TestRunningJobTwice(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron, ctx, cancel := newWithSeconds()
	defer cancel()

	cron.AddFunc("0 0 0 1 1 ?", func(context.Context) {})
	cron.AddFunc("0 0 0 31 12 ?", func(context.Context) {})
	cron.AddFunc("* * * * * ?", func(context.Context) { wg.Done() })

	cron.Start(ctx)

	select {
	case <-time.After(2 * oneSecond):
		t.Error("expected job fires 2 times")
	case <-wait(wg):
	}
}

func TestRunningMultipleSchedules(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron, ctx, cancel := newWithSeconds()
	defer cancel()

	cron.AddFunc("0 0 0 1 1 ?", func(context.Context) {})
	cron.AddFunc("0 0 0 31 12 ?", func(context.Context) {})
	cron.AddFunc("* * * * * ?", func(context.Context) { wg.Done() })
	cron.Schedule(Every(time.Minute), FuncJob(func(context.Context) {}))
	cron.Schedule(Every(time.Second), FuncJob(func(context.Context) { wg.Done() }))
	cron.Schedule(Every(time.Hour), FuncJob(func(context.Context) {}))

	cron.Start(ctx)

	select {
	case <-time.After(2 * oneSecond):
		t.Error("expected job fires 2 times")
	case <-wait(wg):
	}
}

// Test that the cron is run in the local time zone (as opposed to UTC).
func TestLocalTimezone(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron, ctx, cancel := newWithSeconds()
	defer cancel()

	now := time.Now()
	// FIX: Issue #205
	// This calculation doesn't work in seconds 58 or 59.
	// Take the easy way out and sleep.
	if now.Second() >= 58 {
		time.Sleep(2 * time.Second)
		now = time.Now()
	}
	spec := fmt.Sprintf(
		"%d,%d %d %d %d %d ?",
		now.Second()+1, now.Second()+2, now.Minute(), now.Hour(), now.Day(), now.Month(),
	)

	cron.AddFunc(spec, func(context.Context) { wg.Done() })
	cron.Start(ctx)

	select {
	case <-time.After(oneSecond * 2):
		t.Error("expected job fires 2 times")
	case <-wait(wg):
	}
}

// Test that the cron is run in the given time zone (as opposed to local).
func TestNonLocalTimezone(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	loc, err := time.LoadLocation("Atlantic/Cape_Verde")
	if err != nil {
		fmt.Printf("Failed to load time zone Atlantic/Cape_Verde: %+v", err)
		t.Fail()
	}

	now := time.Now().In(loc)
	// FIX: Issue #205
	// This calculation doesn't work in seconds 58 or 59.
	// Take the easy way out and sleep.
	if now.Second() >= 58 {
		time.Sleep(2 * time.Second)
		now = time.Now().In(loc)
	}
	spec := fmt.Sprintf("%d,%d %d %d %d %d ?",
		now.Second()+1, now.Second()+2, now.Minute(), now.Hour(), now.Day(), now.Month())

	cron := New(WithLocation(loc), WithParser(secondParser))
	cron.AddFunc(spec, func(context.Context) { wg.Done() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cron.Start(ctx)

	select {
	case <-time.After(oneSecond * 2):
		t.Error("expected job fires 2 times")
	case <-wait(wg):
	}
}

// Test that adding an invalid job spec returns an error
func TestInvalidJobSpec(t *testing.T) {
	cron := New()
	_, err := cron.AddJob("this will not parse", nil)
	if err == nil {
		t.Errorf("expected an error with invalid spec, got nil")
	}
}

// Test blocking run method behaves as Start()
func TestBlockingRun(t *testing.T) {
	done := make(chan struct{})

	cron, ctx, cancel := newWithSeconds()
	defer cancel()
	cron.AddFunc("* * * * * ?", func(context.Context) { close(done) })

	unblockChan := make(chan struct{})
	go func() {
		cron.Run(ctx)
		close(unblockChan)
	}()

	select {
	case <-time.After(oneSecond):
		t.Error("expected job fires")
	case <-unblockChan:
		t.Error("expected that Run() blocks")
	case <-done:
	}
}

// Test that double-running is a no-op
func TestStartNoop(t *testing.T) {
	tickChan := make(chan struct{}, 2)

	cron, ctx, cancel := newWithSeconds()
	defer cancel()

	cron.AddFunc("* * * * * ?", func(context.Context) {
		tickChan <- struct{}{}
	})

	cron.Start(ctx)

	// Wait for the first firing to ensure the runner is going
	<-tickChan

	cron.Start(context.Background())

	<-tickChan

	// Fail if this job fires again in a short period, indicating a double-run
	select {
	case <-time.After(time.Millisecond):
	case <-tickChan:
		t.Error("expected job fires exactly twice")
	}
}

type testJob struct {
	wg   *sync.WaitGroup
	name string
}

func (t testJob) Run(context.Context) {
	t.wg.Done()
}

// Simple test using Runnables.
func TestJob(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron, ctx, cancel := newWithSeconds()
	defer cancel()

	cron.AddJob("0 0 0 30 Feb ?", testJob{wg, "job0"})
	cron.AddJob("0 0 0 1 1 ?", testJob{wg, "job1"})
	job2, _ := cron.AddJob("* * * * * ?", testJob{wg, "job2"})
	cron.AddJob("1 0 0 1 1 ?", testJob{wg, "job3"})
	cron.Schedule(Every(5*time.Second+5*time.Nanosecond), testJob{wg, "job4"})
	job5 := cron.Schedule(Every(5*time.Minute), testJob{wg, "job5"})

	// Test getting an Entry pre-Start.
	if actualName := cron.Entry(job2).Job.(testJob).name; actualName != "job2" {
		t.Error("wrong job retrieved:", actualName)
	}
	if actualName := cron.Entry(job5).Job.(testJob).name; actualName != "job5" {
		t.Error("wrong job retrieved:", actualName)
	}

	cron.Start(ctx)

	select {
	case <-time.After(oneSecond):
		t.FailNow()
	case <-wait(wg):
	}

	// Ensure the entries are in the right order.
	expecteds := []string{"job2", "job4", "job5", "job1", "job3", "job0"}

	var actuals []string
	for _, entry := range cron.Entries() {
		actuals = append(actuals, entry.Job.(testJob).name)
	}

	for i, expected := range expecteds {
		if actuals[i] != expected {
			t.Fatalf("Jobs not in the right order.  (expected) %s != %s (actual)", expecteds, actuals)
		}
	}

	// Test getting Entries.
	if actualName := cron.Entry(job2).Job.(testJob).name; actualName != "job2" {
		t.Error("wrong job retrieved:", actualName)
	}
	if actualName := cron.Entry(job5).Job.(testJob).name; actualName != "job5" {
		t.Error("wrong job retrieved:", actualName)
	}
}

// Issue #206
// Ensure that the next run of a job after removing an entry is accurate.
func TestScheduleAfterRemoval(t *testing.T) {
	var wg1 sync.WaitGroup
	var wg2 sync.WaitGroup
	wg1.Add(1)
	wg2.Add(1)

	// The first time this job is run, set a timer and remove the other job
	// 750ms later. Correct behavior would be to still run the job again in
	// 250ms, but the bug would cause it to run instead 1s later.

	var calls int
	var mu sync.Mutex

	cron, ctx, cancel := newWithSeconds()
	defer cancel()

	hourJob := cron.Schedule(Every(time.Hour), FuncJob(func(context.Context) {}))
	cron.Schedule(Every(time.Second), FuncJob(func(context.Context) {
		mu.Lock()
		defer mu.Unlock()
		switch calls {
		case 0:
			wg1.Done()
			calls++
		case 1:
			time.Sleep(750 * time.Millisecond)
			cron.Remove(hourJob)
			calls++
		case 2:
			calls++
			wg2.Done()
		case 3:
			panic("unexpected 3rd call")
		}
	}))

	cron.Start(ctx)

	// the first run might be any length of time 0 - 1s, since the schedule
	// rounds to the second. wait for the first run to true up.
	wg1.Wait()

	select {
	case <-time.After(2 * oneSecond):
		t.Error("expected job fires 2 times")
	case <-wait(&wg2):
	}
}

type ZeroSchedule struct{}

func (*ZeroSchedule) Next(time.Time) time.Time {
	return time.Time{}
}

// Tests that job without time does not run
func TestJobWithZeroTimeDoesNotRun(t *testing.T) {
	cron, ctx, cancel := newWithSeconds()
	defer cancel()

	var calls int64
	cron.AddFunc("* * * * * *", func(context.Context) { atomic.AddInt64(&calls, 1) })
	cron.Schedule(new(ZeroSchedule), FuncJob(func(context.Context) { t.Error("expected zero task will not run") }))

	cron.Start(ctx)

	<-time.After(oneSecond)

	if atomic.LoadInt64(&calls) != 1 {
		t.Errorf("called %d times, expected 1\n", calls)
	}
}

func TestWait(t *testing.T) {
	t.Run("no-op when not running", func(t *testing.T) {
		cron, _, cancel := newWithSeconds()
		defer cancel()
		cron.Wait()
	})

	t.Run("handles zero jobs", func(t *testing.T) {
		cron, ctx, cancel := newWithSeconds()
		defer cancel()
		cron.Start(ctx)
		cron.Wait()
		cron.Wait() // multiple calls are ok
	})

	t.Run("blocks until jobs are complete", func(t *testing.T) {
		var count int64

		cron, ctx, cancel := newWithSeconds()
		cron.Start(ctx)

		cron.AddFunc("* * * * * *", func(context.Context) {
			atomic.AddInt64(&count, 1)
		})
		cron.AddFunc("* * * * * *", func(context.Context) {
			time.Sleep(100 * time.Millisecond)
			atomic.AddInt64(&count, 1)
		})

		done := make(chan struct{})
		go func() {
			time.Sleep(time.Second)
			cancel()
			cron.Wait()
			close(done)
		}()

		select {
		case <-done:
			if atomic.LoadInt64(&count) != 2 {
				t.Errorf("expected 2 jobs to run, got %d", count)
			}
		case <-time.After(2 * time.Second):
			t.Error("timed out waiting for jobs to complete")
		}
	})
}

func wait(wg *sync.WaitGroup) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		wg.Wait()
		close(ch)
	}()
	return ch
}

// newWithSeconds returns a Cron with the seconds field enabled.
func newWithSeconds() (*Cron, context.Context, context.CancelFunc) {
	cron := New(WithParser(secondParser), WithChain())
	ctx, cancel := context.WithCancel(context.Background())
	return cron, ctx, cancel
}
