package event

import "sync"

const largeSize4 = 4096
const largeSize = 1024
const mediumSize = 512
const smallSize = 256

var (
	largePool4 = sync.Pool{
		New: func() interface{} { return New(largeSize4) },
	}

	largePool = sync.Pool{
		New: func() interface{} { return New(largeSize) },
	}

	mediumPool = sync.Pool{
		New: func() interface{} { return New(mediumSize) },
	}
	smallPool = sync.Pool{
		New: func() interface{} { return New(smallSize) },
	}
)

// Get return stack node with cached objects (non-thread safe), don't forget call Put after object not needed for reuse
func Get(data []byte) *Event {
	var e *Event
	size := len(data)
	if size == 0 {
		return nil
	} else if size <= smallSize {
		e = smallPool.Get().(*Event)
	} else if size <= mediumSize {
		e = mediumPool.Get().(*Event)
	} else if size <= largeSize {
		e = largePool.Get().(*Event)
	} else if size <= largeSize4 {
		e = largePool4.Get().(*Event)
	} else {
		// TODO: can we need pool for larger messages ?
		return nil
	}
	e.Size = size
	copy(e.Data, data)

	return e
}

func Put(e *Event) {
	if e == nil || e.Size == 0 {
		// non-pooled
		return
	}
	if e.Size <= smallSize {
		smallPool.Put(e)
	} else if e.Size <= mediumSize {
		mediumPool.Put(e)
	} else if e.Size <= largeSize {
		largePool.Put(e)
	} else if e.Size <= largeSize4 {
		largePool4.Put(e)
	}
}
