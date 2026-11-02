package queuer

import "time"

// BackoffFunc returns how long a message should stay hidden before it is
// redelivered, given how many times it has been delivered so far.
// attempt is Message.ReceiveCount and may be 0 when the provider does not
// report delivery counts.
type BackoffFunc func(attempt int) time.Duration

// ConstantBackoff always returns d.
func ConstantBackoff(d time.Duration) BackoffFunc {
	return func(int) time.Duration { return d }
}

// ExponentialBackoff returns base for the first delivery and doubles it for
// every further delivery, capped at max. Adapters additionally clamp the
// result to what their provider accepts (12h for SQS, 600s for Pub/Sub).
func ExponentialBackoff(base, max time.Duration) BackoffFunc {
	return func(attempt int) time.Duration {
		d := base
		for i := 1; i < attempt; i++ {
			d *= 2
			if d >= max || d <= 0 {
				return max
			}
		}
		if d > max {
			return max
		}
		return d
	}
}
