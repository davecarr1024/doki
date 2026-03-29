package node

// RecoveryState describes a replica's recovery posture.
type RecoveryState string

const (
	RecoveryStateHealthy     RecoveryState = "HEALTHY"
	RecoveryStateLagging     RecoveryState = "LAGGING"
	RecoveryStateRecovering  RecoveryState = "RECOVERING"
	RecoveryStateUnavailable RecoveryState = "UNAVAILABLE"
)

func recoverySourceLeader(leaderID string) string {
	if leaderID == "" {
		return ""
	}
	return "leader:" + leaderID
}

func recoverySourceBootstrap(shardID string) string {
	if shardID == "" {
		return ""
	}
	return "bootstrap:" + shardID
}
