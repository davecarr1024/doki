package node

import (
	"context"
	"errors"
	"fmt"

	commonv1 "github.com/davecarr1024/doki/gen/doki/common/v1"
	nodev1 "github.com/davecarr1024/doki/gen/doki/node/v1"
	"github.com/davecarr1024/doki/internal/replicationlog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type grpcServer struct {
	nodev1.UnimplementedNodeServiceServer
	s *Server
}

func (g *grpcServer) Put(ctx context.Context, req *nodev1.PutRequest) (*nodev1.PutResponse, error) {
	return g.handleWrite(ctx, req.ShardId, "put", string(req.Key), string(req.Value))
}

func (g *grpcServer) Delete(ctx context.Context, req *nodev1.DeleteRequest) (*nodev1.DeleteResponse, error) {
	resp, err := g.handleWrite(ctx, req.ShardId, "delete", string(req.Key), "")
	if err != nil {
		return nil, err
	}
	return &nodev1.DeleteResponse{Result: mapWriteResult(resp.Result), LeaderHint: resp.LeaderHint}, nil
}

func (g *grpcServer) Get(ctx context.Context, req *nodev1.GetRequest) (*nodev1.GetResponse, error) {
	shardID := req.ShardId
	g.s.mu.RLock()
	replica := g.s.replicas[shardID]
	g.s.mu.RUnlock()
	if replica == nil {
		return nil, status.Errorf(codes.NotFound, "shard not found")
	}

	snap := replica.StatusSnapshot()
	if snap.Role != RoleLeader {
		return &nodev1.GetResponse{
			Result:     nodev1.GetResponse_RESULT_NOT_LEADER,
			LeaderHint: snap.LeaderID,
		}, nil
	}
	if !snap.IsReady {
		return &nodev1.GetResponse{Result: nodev1.GetResponse_RESULT_NOT_READY}, nil
	}

	val, ok := replica.SM.Get(string(req.Key))
	if !ok {
		return &nodev1.GetResponse{Result: nodev1.GetResponse_RESULT_NOT_FOUND}, nil
	}
	return &nodev1.GetResponse{Result: nodev1.GetResponse_RESULT_OK, Value: []byte(val)}, nil
}

func (g *grpcServer) Replicate(ctx context.Context, req *nodev1.ReplicateRequest) (*nodev1.ReplicateResponse, error) {
	shardID := req.ShardId
	g.s.mu.RLock()
	replica := g.s.replicas[shardID]
	g.s.mu.RUnlock()
	if replica == nil {
		return nil, status.Errorf(codes.NotFound, "shard not found")
	}

	opType, key, value := opFromProto(req.Op)
	replica.mu.Lock()
	if req.Term < replica.Term {
		term := replica.Term
		replica.mu.Unlock()
		g.s.m.ReplicationsTotal.WithLabelValues(shardID, "stale_term").Inc()
		return &nodev1.ReplicateResponse{Success: false, Term: term}, nil
	}
	if req.Term > replica.Term {
		replica.Term = req.Term
		if replica.Role != RoleFollower {
			replica.Role = RoleFollower
		}
	}
	if req.Version <= replica.Version {
		term := replica.Term
		replica.mu.Unlock()
		g.s.m.ReplicationsTotal.WithLabelValues(shardID, "duplicate").Inc()
		return &nodev1.ReplicateResponse{Success: true, Term: term}, nil
	}
	if req.Version != replica.Version+1 {
		term := replica.Term
		replica.IsReady = false
		replica.RecoveryState = RecoveryStateLagging
		replica.RecoverySource = recoverySourceLeader(replica.LeaderID)
		replica.LastLeaderContact = g.s.clock.Now()
		replica.SM.ResetLog()
		replica.mu.Unlock()
		g.s.m.ReplicationsTotal.WithLabelValues(shardID, "gap").Inc()
		return &nodev1.ReplicateResponse{Success: false, Term: term}, nil
	}

	entry := replicationlog.Entry{
		Term:    req.Term,
		Version: req.Version,
		Op:      opType,
		Key:     key,
		Value:   value,
	}
	if err := replica.SM.Apply(entry, ApplyWithWAL); err != nil {
		replica.mu.Unlock()
		g.s.m.ReplicationsTotal.WithLabelValues(shardID, "wal_error").Inc()
		return nil, status.Errorf(codes.Internal, "wal append: %v", err)
	}
	replica.IsReady = true
	replica.RecoveryState = RecoveryStateHealthy
	replica.RecoverySource = ""
	replica.LastLeaderContact = g.s.clock.Now()
	replica.mu.Unlock()

	g.s.m.ReplicationsTotal.WithLabelValues(shardID, "apply_ok").Inc()
	return &nodev1.ReplicateResponse{Success: true, Term: req.Term}, nil
}

func (g *grpcServer) Recover(ctx context.Context, req *nodev1.RecoverRequest) (*nodev1.RecoverResponse, error) {
	shardID := req.ShardId
	sinceVersion := req.SinceVersion

	g.s.mu.RLock()
	replica := g.s.replicas[shardID]
	g.s.mu.RUnlock()
	if replica == nil {
		return nil, status.Errorf(codes.NotFound, "shard not found")
	}

	replica.mu.RLock()
	if replica.Role != RoleLeader {
		replica.mu.RUnlock()
		return nil, status.Errorf(codes.FailedPrecondition, "not leader")
	}

	snap := replica.SM.Snapshot()
	leaderVersion := snap.Version
	term := snap.Term

	resp := &nodev1.RecoverResponse{Version: leaderVersion}

	if sinceVersion > leaderVersion {
		replica.mu.RUnlock()
		resp.Type = nodev1.RecoverResponse_TYPE_SNAPSHOT
		resp.Term = term
		resp.Kv = kvToProto(snap.KV)
		return resp, nil
	}

	if leaderVersion == sinceVersion {
		replica.mu.RUnlock()
		resp.Type = nodev1.RecoverResponse_TYPE_ENTRIES
		return resp, nil
	}

	entries, ok := replica.SM.EntriesSince(sinceVersion)
	if ok && (len(entries) > 0 || leaderVersion == sinceVersion) {
		replica.mu.RUnlock()
		resp.Type = nodev1.RecoverResponse_TYPE_ENTRIES
		resp.Entries = entriesToProto(entries)
		return resp, nil
	}

	replica.mu.RUnlock()
	resp.Type = nodev1.RecoverResponse_TYPE_SNAPSHOT
	resp.Term = term
	resp.Kv = kvToProto(snap.KV)
	return resp, nil
}

func (g *grpcServer) ForceRecover(ctx context.Context, req *nodev1.ForceRecoverRequest) (*nodev1.ForceRecoverResponse, error) {
	shardID := req.ShardId
	g.s.mu.RLock()
	replica := g.s.replicas[shardID]
	g.s.mu.RUnlock()
	if replica == nil {
		return nil, status.Errorf(codes.NotFound, "shard not found")
	}
	replica.mu.Lock()
	replica.IsReady = false
	replica.RecoveryState = RecoveryStateLagging
	replica.RecoverySource = recoverySourceLeader(replica.LeaderID)
	replica.SM.ResetLog()
	replica.LastLeaderContact = g.s.clock.Now()
	replica.mu.Unlock()
	return &nodev1.ForceRecoverResponse{}, nil
}

func (g *grpcServer) RequestVote(ctx context.Context, req *nodev1.VoteRequest) (*nodev1.VoteResponse, error) {
	shardID := req.ShardId
	g.s.mu.RLock()
	replica := g.s.replicas[shardID]
	g.s.mu.RUnlock()
	if replica == nil {
		return nil, status.Errorf(codes.NotFound, "shard not found")
	}

	replica.mu.Lock()
	defer replica.mu.Unlock()

	myTerm := replica.Term
	if req.Term < myTerm {
		return &nodev1.VoteResponse{Term: myTerm, VoteGranted: false}, nil
	}
	if req.Term > myTerm {
		replica.Term = req.Term
		replica.VotedFor = ""
		replica.VotedForTerm = 0
		if replica.Role != RoleFollower {
			replica.Role = RoleFollower
		}
		myTerm = req.Term
	}
	if replica.VotedForTerm == req.Term && replica.VotedFor != "" && replica.VotedFor != req.CandidateId {
		return &nodev1.VoteResponse{Term: myTerm, VoteGranted: false}, nil
	}
	if req.Version < replica.Version {
		return &nodev1.VoteResponse{Term: myTerm, VoteGranted: false}, nil
	}

	replica.VotedFor = req.CandidateId
	replica.VotedForTerm = req.Term
	replica.LastLeaderContact = g.s.clock.Now()
	return &nodev1.VoteResponse{Term: myTerm, VoteGranted: true}, nil
}

func (g *grpcServer) LeaderHeartbeat(ctx context.Context, req *nodev1.LeaderHeartbeatRequest) (*nodev1.LeaderHeartbeatResponse, error) {
	shardID := req.ShardId
	g.s.mu.RLock()
	replica := g.s.replicas[shardID]
	g.s.mu.RUnlock()
	if replica == nil {
		return nil, status.Errorf(codes.NotFound, "shard not found")
	}

	replica.mu.Lock()
	myTerm := replica.Term
	if req.Term < myTerm {
		replica.mu.Unlock()
		return &nodev1.LeaderHeartbeatResponse{Term: myTerm}, nil
	}
	if req.Term > myTerm {
		replica.Term = req.Term
		if replica.Role != RoleFollower {
			replica.Role = RoleFollower
		}
	}
	replica.LeaderID = req.LeaderId
	replica.LastLeaderContact = g.s.clock.Now()
	myTerm = replica.Term
	replica.mu.Unlock()

	return &nodev1.LeaderHeartbeatResponse{Term: myTerm}, nil
}

func (g *grpcServer) GetStatus(ctx context.Context, req *nodev1.GetStatusRequest) (*nodev1.GetStatusResponse, error) {
	g.s.mu.RLock()
	shards := make([]*nodev1.ShardStatus, 0, len(g.s.replicas))
	for _, rep := range g.s.replicas {
		snap := rep.StatusSnapshot()
		shards = append(shards, &nodev1.ShardStatus{
			ShardId:        snap.ShardID,
			Role:           string(snap.Role),
			Term:           snap.Term,
			Version:        snap.Version,
			IsReady:        snap.IsReady,
			RecoveryState:  string(snap.RecoveryState),
			RecoverySource: snap.RecoverySource,
			Peers:          append([]string(nil), snap.Peers...),
		})
	}
	nodeID := g.s.cfg.Node.ID
	uptime := g.s.clock.Now().Sub(g.s.startedAt).Seconds()
	shardMapVersion := g.s.shardMapVersion
	g.s.mu.RUnlock()

	return &nodev1.GetStatusResponse{
		NodeId:          nodeID,
		UptimeSeconds:   uptime,
		Shards:          shards,
		ShardMapVersion: shardMapVersion,
	}, nil
}

func (g *grpcServer) handleWrite(ctx context.Context, shardID, op, key, value string) (*nodev1.PutResponse, error) {
	g.s.mu.RLock()
	replica := g.s.replicas[shardID]
	g.s.mu.RUnlock()
	if replica == nil {
		return nil, status.Errorf(codes.NotFound, "shard not found")
	}
	snap, err := ensureLeaderReady(replica)
	if err != nil {
		if errors.Is(err, errNotLeader) {
			return &nodev1.PutResponse{
				Result:     nodev1.PutResponse_RESULT_NOT_LEADER,
				LeaderHint: snap.LeaderID,
			}, nil
		}
		return &nodev1.PutResponse{
			Result: nodev1.PutResponse_RESULT_NOT_READY,
		}, nil
	}
	result, err := g.s.leaderWrite(ctx, replica, KVRequest{Op: op, Key: key, Value: value})
	if err != nil {
		if errors.Is(err, errShardMapStale) {
			return &nodev1.PutResponse{Result: nodev1.PutResponse_RESULT_NOT_READY}, nil
		}
		g.s.m.WritesTotal.WithLabelValues(shardID, "quorum_unavailable").Inc()
		return &nodev1.PutResponse{Result: nodev1.PutResponse_RESULT_QUORUM_UNAVAILABLE}, nil
	}
	_ = grpc.SetHeader(ctx, metadata.Pairs(
		"x-doki-applied-version", fmt.Sprintf("%d", result.AppliedVersion),
		"x-doki-quorum", fmt.Sprintf("%d", result.Quorum),
	))
	g.s.m.WritesTotal.WithLabelValues(shardID, "ok").Inc()
	return &nodev1.PutResponse{Result: nodev1.PutResponse_RESULT_OK}, nil
}

func mapWriteResult(in nodev1.PutResponse_Result) nodev1.DeleteResponse_Result {
	switch in {
	case nodev1.PutResponse_RESULT_OK:
		return nodev1.DeleteResponse_RESULT_OK
	case nodev1.PutResponse_RESULT_NOT_LEADER:
		return nodev1.DeleteResponse_RESULT_NOT_LEADER
	case nodev1.PutResponse_RESULT_QUORUM_UNAVAILABLE:
		return nodev1.DeleteResponse_RESULT_QUORUM_UNAVAILABLE
	case nodev1.PutResponse_RESULT_NOT_READY:
		return nodev1.DeleteResponse_RESULT_NOT_READY
	default:
		return nodev1.DeleteResponse_RESULT_UNSPECIFIED
	}
}

func opFromProto(op *commonv1.Operation) (string, string, string) {
	if op == nil {
		return "", "", ""
	}
	switch op.Type {
	case commonv1.Operation_TYPE_PUT:
		return "put", string(op.Key), string(op.Value)
	case commonv1.Operation_TYPE_DELETE:
		return "delete", string(op.Key), ""
	default:
		return "", string(op.Key), string(op.Value)
	}
}

func entriesToProto(entries []replicationlog.Entry) []*nodev1.LogEntry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]*nodev1.LogEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, &nodev1.LogEntry{
			Term:    e.Term,
			Version: e.Version,
			Op:      opToProto(e.Op, e.Key, e.Value),
		})
	}
	return out
}

func kvToProto(kv map[string]string) []*commonv1.KVEntry {
	if len(kv) == 0 {
		return nil
	}
	out := make([]*commonv1.KVEntry, 0, len(kv))
	for k, v := range kv {
		out = append(out, &commonv1.KVEntry{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}
	return out
}

func opToProto(op, key, value string) *commonv1.Operation {
	switch op {
	case "put":
		return &commonv1.Operation{
			Type:  commonv1.Operation_TYPE_PUT,
			Key:   []byte(key),
			Value: []byte(value),
		}
	case "delete":
		return &commonv1.Operation{
			Type: commonv1.Operation_TYPE_DELETE,
			Key:  []byte(key),
		}
	default:
		return &commonv1.Operation{
			Type:  commonv1.Operation_TYPE_UNSPECIFIED,
			Key:   []byte(key),
			Value: []byte(value),
		}
	}
}
