package node

import "errors"

var (
	errNotLeader = errors.New("not leader")
	errNotReady  = errors.New("not ready")
)

func ensureLeaderReady(replica *ReplicaState) (ReplicaStatusSnapshot, error) {
	snap := replica.StatusSnapshot()
	if snap.Role != RoleLeader {
		return snap, errNotLeader
	}
	if !snap.IsReady {
		return snap, errNotReady
	}
	return snap, nil
}
