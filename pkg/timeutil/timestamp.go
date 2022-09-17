package timeutil

import "time"

type Time struct {
	t time.Time
	s string
}

func (t Time) Time() time.Time {
	return t.t
}

func (t Time) String() string {
	return t.s
}

func String(t time.Time) string {
	return t.Format(time.RFC3339Nano)
}

func Timestamp(t time.Time) Time {
	return Time{
		t: t,
		s: String(t),
	}
}

func Now() Time {
	now := time.Now()
	return Timestamp(now)
}

func Parse(layout, s string) (Time, error) {
	if t, err := time.Parse(layout, s); err != nil {
		return Time{}, err
	} else {
		return Time{t: t, s: s}, nil
	}
}
