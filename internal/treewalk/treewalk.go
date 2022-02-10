// Package treewalk provides useful primitive for efficiently walking tree-like structures in parallel with a controlled degree of parallelism.
package treewalk

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"
)

// ItemFunc is a user-provided callback that processes given item and reports all child nodes to be processed by calling the
// provided ReportChildFunc. The children are processed in concurrent goroutines or inline if all concurrent goroutines are busy.
type ItemFunc func(ctx context.Context, it interface{}, onChild ReportChildFunc) error

type ReportChildFunc func(ctx context.Context, it interface{}) error

type queue struct {
	ch       chan interface{}
	mon      *sync.Cond
	queued   int
	active   int
	itemFunc ItemFunc
}

func (q *queue) queueOrRun(ctx context.Context, it interface{}) error {
	var queued bool

	q.mon.L.Lock()
	select {
	case q.ch <- it:
		queued = true
		q.queued++
		q.mon.Signal()
	default:
	}

	q.mon.L.Unlock()

	if queued {
		return nil
	}

	// no more room in the channel, use current goroutine stack and just do the discovery recursively
	return q.itemFunc(ctx, it, q.queueOrRun)
}

func (q *queue) dequeueDisc(ctx context.Context) (interface{}, bool) {
	q.mon.L.Lock()
	defer q.mon.L.Unlock()

	for q.queued == 0 && q.active > 0 {
		// no items in queue, but some another discovery is in progress, they may add more.
		q.mon.Wait()
	}

	// no items in queue, no workers are active, no more work.
	if q.queued == 0 {
		return nil, false
	}

	q.active++
	q.queued--

	it := <-q.ch

	return it, true
}

func (q *queue) completedDisc(ctx context.Context) {
	q.mon.L.Lock()
	defer q.mon.L.Unlock()

	q.active--
	q.mon.Broadcast()
}

// InParallel invokes the provided callback for each item in the provided list possibly in parallel, up to the
// specified degree of parallelism and ensures that all children reported by the callback are also processed
// before the call returns. If all item callbacks succeed, the value is nil. If an ItemFunc fails, the function
// returns the provided error and cancels remaining iterations.
// If multiple item functions fail, an aribtrarily-chosen errors is returned.
func InParallel(ctx context.Context, parallel int, disc ItemFunc, items []interface{}) error {
	q := &queue{
		mon:      sync.NewCond(&sync.Mutex{}),
		itemFunc: disc,
	}

	eg, ctx := errgroup.WithContext(ctx)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// add artificial in-progress discovery to prevent workers from shutting down prematurely.
	q.ch = make(chan interface{}, 100)
	q.active = 1

	for i := 0; i < parallel; i++ {
		eg.Go(func() error {
			for {
				it, ok := q.dequeueDisc(ctx)
				if !ok {
					return nil
				}

				if err := q.itemFunc(ctx, it, q.queueOrRun); err != nil {
					return err
				}

				q.completedDisc(ctx)
			}
		})
	}

	for _, it := range items {
		if err := q.queueOrRun(ctx, it); err != nil {
			return err
		}
	}

	q.completedDisc(ctx)

	return eg.Wait()
}
