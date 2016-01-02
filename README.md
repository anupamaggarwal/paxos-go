A simple Paxos implementation in go


API's supported

1. px = paxos.Make(peers []string, me int)
2. px.Start(seq int, v interface{}) // start agreement on new instance
3. px.Status(seq int) (fate Fate, v interface{}) // get info about an instance
4. px.Done(seq int) // ok to forget all instances <= seq
5. px.Max() int // highest instance seq known, or -1
6. px.Min() int // instances before this have been forgotten
