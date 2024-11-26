package raft

import "github.com/pingcap-incubator/tinykv/log"

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

func (pr *Progress) MaybeUpdate(n uint64) bool {
	var updated bool
	if pr.Match < n {
		pr.Match = n
		updated = true
		//pr.ProbeAcked()
	}
	pr.Next = max(pr.Next, n+1)
	return updated
}

func (pr *Progress) MaybeDecrTo(rejected, nextHint uint64) bool {
	if rejected <= pr.Match {
		// The rejection must be stale if the progress has matched and "rejected"
		// is smaller than "match".
		log.Debugf("Don't decr nextIndex from %d to %d, because rejected %d <= matchIndex %d", pr.Next, nextHint, rejected, pr.Match)
		return false
	}
	pr.Next = nextHint
	return true
}
