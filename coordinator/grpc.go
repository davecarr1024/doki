package coordinator

import (
	"context"

	commonv1 "github.com/davecarr1024/doki/gen/doki/common/v1"
	coordinatorv1 "github.com/davecarr1024/doki/gen/doki/coordinator/v1"
	"github.com/davecarr1024/doki/internal/shardmap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type grpcServer struct {
	coordinatorv1.UnimplementedCoordinatorServiceServer
	s *Server
}

func (g *grpcServer) Heartbeat(ctx context.Context, req *coordinatorv1.HeartbeatRequest) (*coordinatorv1.HeartbeatResponse, error) {
	shards := make([]ShardStatus, 0, len(req.Shards))
	for _, sh := range req.Shards {
		shards = append(shards, ShardStatus{
			ShardID: sh.ShardId,
			Role:    sh.Role,
			Version: sh.Version,
			IsReady: sh.IsReady,
			Term:    sh.Term,
		})
	}
	if err := g.s.membership.RecordHeartbeat(req.NodeId, shards); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	g.s.m.HeartbeatReceivedTotal.WithLabelValues(req.NodeId).Inc()
	smVersion, _ := g.s.shards.Snapshot()
	return &coordinatorv1.HeartbeatResponse{ShardMapVersion: smVersion}, nil
}

func (g *grpcServer) GetShardMap(ctx context.Context, req *coordinatorv1.GetShardMapRequest) (*coordinatorv1.GetShardMapResponse, error) {
	version, shards := g.s.shards.Snapshot()
	allNodes := g.s.membership.All()
	nodeAddrs := make(map[string]string, len(allNodes))
	for _, ns := range allNodes {
		nodeAddrs[ns.ID] = ns.Address
	}
	return &coordinatorv1.GetShardMapResponse{
		ShardMap:      &commonv1.ShardMap{Version: version, Shards: shardInfoToProto(shards)},
		NodeAddresses: nodeAddrs,
	}, nil
}

func (g *grpcServer) WhereIsLeader(ctx context.Context, req *coordinatorv1.WhereIsLeaderRequest) (*coordinatorv1.WhereIsLeaderResponse, error) {
	leader, err := g.s.shards.LeaderFor(req.ShardId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	ns, err := g.s.membership.Get(leader)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "leader node not found")
	}
	return &coordinatorv1.WhereIsLeaderResponse{
		LeaderNodeId: leader,
		Address:      ns.Address,
		Term:         g.s.leader.TermForShard(req.ShardId),
	}, nil
}

func (g *grpcServer) GetStatus(ctx context.Context, req *coordinatorv1.GetStatusRequest) (*coordinatorv1.GetStatusResponse, error) {
	now := g.s.clock.Now()
	nodeStatuses := g.s.membership.All()
	nodes := make([]*coordinatorv1.NodeHealthEntry, 0, len(nodeStatuses))
	for _, n := range nodeStatuses {
		lastMs := int64(-1)
		if !n.LastHeartbeatAt.IsZero() {
			lastMs = now.Sub(n.LastHeartbeatAt).Milliseconds()
		}
		nodes = append(nodes, &coordinatorv1.NodeHealthEntry{
			NodeId:          n.ID,
			Address:         n.Address,
			IsAlive:         n.IsAlive,
			LastHeartbeatMs: lastMs,
		})
	}
	version, shards := g.s.shards.Snapshot()
	return &coordinatorv1.GetStatusResponse{
		ShardMapVersion: version,
		Nodes:           nodes,
		Shards:          shardInfoToProto(shards),
	}, nil
}

func (g *grpcServer) NotifyLeader(ctx context.Context, req *coordinatorv1.NotifyLeaderRequest) (*coordinatorv1.NotifyLeaderResponse, error) {
	accepted := g.s.leader.NotifyLeader(req.ShardId, req.LeaderId, req.Term)
	currentTerm := g.s.leader.TermForShard(req.ShardId)
	if accepted {
		g.s.m.NotifyLeaderTotal.WithLabelValues("accepted").Inc()
	} else {
		g.s.m.NotifyLeaderTotal.WithLabelValues("rejected").Inc()
	}
	return &coordinatorv1.NotifyLeaderResponse{Accepted: accepted, CurrentTerm: currentTerm}, nil
}

func shardInfoToProto(shards []shardmap.ShardInfo) []*commonv1.ShardInfo {
	out := make([]*commonv1.ShardInfo, 0, len(shards))
	for _, sh := range shards {
		out = append(out, &commonv1.ShardInfo{
			ShardId:  sh.ID,
			Leader:   sh.Leader,
			Replicas: append([]string(nil), sh.Replicas...),
		})
	}
	return out
}
