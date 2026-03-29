package node

import (
	"context"
	"fmt"
	"time"

	"github.com/davecarr1024/doki/coordinator"
	commonv1 "github.com/davecarr1024/doki/gen/doki/common/v1"
	coordinatorv1 "github.com/davecarr1024/doki/gen/doki/coordinator/v1"
	"github.com/davecarr1024/doki/internal/shardmap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (s *Server) dialCoordinator(ctx context.Context, timeout time.Duration) (*grpc.ClientConn, coordinatorv1.CoordinatorServiceClient, error) {
	ctx, cancel := context.WithTimeout(ctx, grpcDeadline(timeout))
	defer cancel()
	conn, err := grpc.DialContext(ctx, s.cfg.CoordinatorAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, nil, fmt.Errorf("dial coordinator: %w", err)
	}
	return conn, coordinatorv1.NewCoordinatorServiceClient(conn), nil
}

func shardMapResponseFromProto(resp *coordinatorv1.GetShardMapResponse) coordinator.ShardMapResponse {
	if resp == nil || resp.ShardMap == nil {
		return coordinator.ShardMapResponse{}
	}
	return coordinator.ShardMapResponse{
		Version:       resp.ShardMap.Version,
		Shards:        shardInfoFromProto(resp.ShardMap.Shards),
		NodeAddresses: resp.NodeAddresses,
	}
}

func shardInfoFromProto(shards []*commonv1.ShardInfo) []shardmap.ShardInfo {
	if len(shards) == 0 {
		return nil
	}
	out := make([]shardmap.ShardInfo, 0, len(shards))
	for _, sh := range shards {
		out = append(out, shardmap.ShardInfo{
			ID:       sh.ShardId,
			Leader:   sh.Leader,
			Replicas: append([]string(nil), sh.Replicas...),
		})
	}
	return out
}

func shardHeartbeatsFromStatus(shards []coordinator.ShardStatus) []*coordinatorv1.ShardHeartbeat {
	if len(shards) == 0 {
		return nil
	}
	out := make([]*coordinatorv1.ShardHeartbeat, 0, len(shards))
	for _, sh := range shards {
		out = append(out, &coordinatorv1.ShardHeartbeat{
			ShardId: sh.ShardID,
			Role:    sh.Role,
			Term:    sh.Term,
			Version: sh.Version,
			IsReady: sh.IsReady,
		})
	}
	return out
}

func grpcDeadline(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		return 2 * time.Second
	}
	return timeout
}
