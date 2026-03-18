// Package clock provides a Clock interface for testable time operations.
//
// All components that use time should accept a Clock rather than calling
// time.Now() or time.Sleep() directly. This allows unit tests to control
// time precisely without sleeping.
package clock

import "time"

// Clock abstracts time operations so they can be faked in tests.
type Clock interface {
	Now() time.Time
	Sleep(d time.Duration)
	After(d time.Duration) <-chan time.Time
}

// Real is the production clock backed by the system clock.
type Real struct{}

func (Real) Now() time.Time                         { return time.Now() }
func (Real) Sleep(d time.Duration)                  { time.Sleep(d) }
func (Real) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Fake is a manually-controlled clock for use in unit tests.
// Advance time by calling Advance(); sleeping goroutines are unblocked
// when the fake clock passes their target time.
//
// Note: Fake is intentionally simple. It does not support concurrent
// goroutines waiting on After() channels — use it for single-threaded
// logic tests only.
type Fake struct {
	current time.Time
}

// NewFake creates a fake clock starting at the given time.
func NewFake(start time.Time) *Fake {
	return &Fake{current: start}
}

// NewFakeNow creates a fake clock starting at time.Now().
func NewFakeNow() *Fake {
	return NewFake(time.Now())
}

// Now returns the fake clock's current time.
func (f *Fake) Now() time.Time {
	return f.current
}

// Advance moves the fake clock forward by d.
func (f *Fake) Advance(d time.Duration) {
	f.current = f.current.Add(d)
}

// Sleep does nothing in the fake clock (returns immediately).
// Use Advance() to move time forward in tests.
func (f *Fake) Sleep(_ time.Duration) {}

// After returns a channel that immediately receives the current time.
// In a fake clock, all timers fire immediately.
func (f *Fake) After(_ time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	ch <- f.current
	return ch
}
