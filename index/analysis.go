//  Copyright (c) 2015 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package index

import "github.com/wrble/flock/document"

type IndexRow interface {
	Table() string
	Key() []byte

	ValueSize() int
	ValueTo([]byte) (int, error)
	Value() []byte
}

type AnalysisResult struct {
	DocID string
	Rows  []IndexRow
}

type AnalysisWork struct {
	i  Index
	d  *document.Document
	rc chan *AnalysisResult
}

func NewAnalysisWork(i Index, d *document.Document, rc chan *AnalysisResult) *AnalysisWork {
	return &AnalysisWork{
		i:  i,
		d:  d,
		rc: rc,
	}
}

type AnalysisQueue struct {
	queue chan *AnalysisWork
	done  chan struct{}
}

func (q *AnalysisQueue) Queue(work *AnalysisWork) {
	q.queue <- work
}

func (q *AnalysisQueue) Close() {
	close(q.done)
}

func NewAnalysisQueue(numWorkers int) *AnalysisQueue {
	rv := AnalysisQueue{
		queue: make(chan *AnalysisWork),
		done:  make(chan struct{}),
	}
	for i := 0; i < numWorkers; i++ {
		go AnalysisWorker(rv)
	}
	return &rv
}

func AnalysisWorker(q AnalysisQueue) {
	// read work off the queue
	for {
		select {
		case <-q.done:
			return
		case w := <-q.queue:
			r := w.i.Analyze(w.d)
			w.rc <- r
		}
	}
}
