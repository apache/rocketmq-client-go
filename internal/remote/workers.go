package remote

import (
	"errors"
	"sync"
	"sync/atomic"
)

const (
	defaultWorkersNum     = 10
	defaultWriteBuffSize  = 32
	defaultHandleBuffSize = 16
	defaultTimerBuffSize  = 8
)

type workerFunc func()

// WorkerPool is a pool of go-routines running functions.
type WorkerPool struct {
	index     uint32
	workers   []*worker
	closeChan chan struct{}
}

var (
	globalWorkerPool         *WorkerPool
	globalWorkerPoolInitLock sync.RWMutex
)

func initGlobalWorkPool(size int) {
	if globalWorkerPool != nil {
		globalWorkerPool.Close()
	}
	globalWorkerPool = newWorkerPool(size)
}

// WorkerPoolInstance returns the global pool.
func WorkerPoolInstance() *WorkerPool {
	if globalWorkerPool != nil {
		return globalWorkerPool
	}
	globalWorkerPoolInitLock.Lock()
	if globalWorkerPool != nil {
		globalWorkerPoolInitLock.Unlock()
		return globalWorkerPool
	}
	initGlobalWorkPool(0)
	globalWorkerPoolInitLock.Unlock()
	return globalWorkerPool
}

func newWorkerPool(vol int) *WorkerPool {
	if vol <= 0 {
		vol = defaultWorkersNum
	}

	pool := &WorkerPool{
		index:     0,
		workers:   make([]*worker, vol),
		closeChan: make(chan struct{}),
	}

	for i := range pool.workers {
		pool.workers[i] = newWorker(i, 1024, pool.closeChan)
		if pool.workers[i] == nil {
			panic("worker nil")
		}
	}

	return pool
}

// Put appends a function to some worker's channel.
func (wp *WorkerPool) Put(code uint32, cb func())  {
	if code == 0 {
		code = atomic.AddUint32(&wp.index, 1)
	}
	wp.workers[code&uint32(len(wp.workers)-1)].put(workerFunc(cb))

}

// Close closes the pool, stopping it from executing functions.
func (wp *WorkerPool) Close() {
	close(wp.closeChan)
}

// Size returns the size of pool.
func (wp *WorkerPool) Size() int {
	return len(wp.workers)
}

type worker struct {
	index        int
	callbackChan chan workerFunc
	closeChan    chan struct{}
}

func newWorker(i int, c int, closeChan chan struct{}) *worker {
	w := &worker{
		index:        i,
		callbackChan: make(chan workerFunc, c),
		closeChan:    closeChan,
	}
	go w.start()
	return w
}

func (w *worker) start() {
	for {
		select {
		case <-w.closeChan:
			return
		case cb := <-w.callbackChan:
			cb()
		}
	}
}

func (w *worker) put(cb workerFunc) error {
	select {
	case w.callbackChan <- cb:
		return nil
	default:
		return errors.New("would block")
	}
}
