package node

import "time"

// QuorumInfo captures the leader's view of eligible peers for a write.
type QuorumInfo struct {
	LivePeers       map[string]string
	TotalEligible   int
	Quorum          int
	NeededFollowers int
}

// WriteResult is the consistent result returned by the write path.
type WriteResult struct {
	AppliedVersion uint64
	Quorum         int
	AckedFollowers int
	TotalEligible  int
}

// planQuorum determines which peers are eligible for quorum based on liveness.
func planQuorum(peerAddrs map[string]string, peerLast map[string]time.Time, now time.Time, livenessWindow time.Duration) QuorumInfo {
	livePeers := make(map[string]string, len(peerAddrs))
	if livenessWindow <= 0 {
		for id, addr := range peerAddrs {
			livePeers[id] = addr
		}
	} else {
		for id, addr := range peerAddrs {
			if last, ok := peerLast[id]; ok && now.Sub(last) <= livenessWindow {
				livePeers[id] = addr
			}
		}
	}
	// Total eligible replicas = live followers + leader.
	totalEligible := len(livePeers) + 1
	quorum := totalEligible/2 + 1
	needed := quorum - 1
	return QuorumInfo{
		LivePeers:       livePeers,
		TotalEligible:   totalEligible,
		Quorum:          quorum,
		NeededFollowers: needed,
	}
}

func (q QuorumInfo) hasLiveQuorum() bool {
	return len(q.LivePeers) >= q.NeededFollowers
}

func (q QuorumInfo) result(appliedVersion uint64, acked int) WriteResult {
	return WriteResult{
		AppliedVersion: appliedVersion,
		Quorum:         q.Quorum,
		AckedFollowers: acked,
		TotalEligible:  q.TotalEligible,
	}
}
