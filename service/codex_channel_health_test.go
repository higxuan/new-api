package service

import (
	"context"
	"errors"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/model"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

func TestCodexChannelHealthThresholdAndRecovery(t *testing.T) {
	s := miniredis.RunT(t)
	prevEnabled, prevRDB := common.RedisEnabled, common.RDB
	client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	common.RedisEnabled, common.RDB = true, client
	t.Cleanup(func() { common.RedisEnabled, common.RDB = prevEnabled, prevRDB; _ = client.Close() })
	failed := relaycommon.NewStreamStatus()
	failed.MarkFailed("server_error", "server_error", 500)
	failed.SetEndReason(relaycommon.StreamEndReasonEOF, nil)
	successful := relaycommon.NewStreamStatus()
	successful.MarkCompleted()
	successful.SetEndReason(relaycommon.StreamEndReasonEOF, nil)
	cancelled := relaycommon.NewStreamStatus()
	cancelled.MarkFailed("cancelled", "", 0)
	cancelled.SetEndReason(relaycommon.StreamEndReasonClientGone, context.Canceled)
	for range 2 {
		RecordCodexStreamHealth(1, failed)
	}
	require.False(t, IsChannelModelCoolingDown(1, "model-a"))
	RecordCodexStreamHealth(1, cancelled)
	RecordCodexStreamHealth(1, successful) // completed EOF resets the consecutive failures
	RecordCodexStreamHealth(1, failed)
	require.False(t, IsChannelModelCoolingDown(1, "model-a"))
	for range 2 {
		RecordCodexStreamHealth(1, failed)
	}
	require.True(t, IsChannelModelCoolingDown(1, "model-a"))
	require.True(t, IsChannelModelCoolingDown(1, "model-b"))
	require.False(t, IsChannelModelCoolingDown(2, "model-a"))
	_, key := codexChannelHealthKeys(1)
	s.FastForward(time.Minute)
	RecordCodexStreamHealth(1, failed)
	RecordCodexStreamHealth(1, successful)
	require.Equal(t, 4*time.Minute, s.TTL(key)) // neither late success nor failure extends/clears cooldown
	s.FastForward(4 * time.Minute)
	require.False(t, IsChannelModelCoolingDown(1, "model-a"))
	RecordCodexStreamHealth(1, successful)
	require.False(t, s.Exists(key))
}

func TestCodexChannelCooldownRoutesToOtherAccountThenOverflow(t *testing.T) {
	db := setupChannelSelectAutoGroupsTest(t)
	s := miniredis.RunT(t)
	prevEnabled, prevRDB := common.RedisEnabled, common.RDB
	client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	common.RedisEnabled, common.RDB = true, client
	t.Cleanup(func() { common.RedisEnabled, common.RDB = prevEnabled, prevRDB; _ = client.Close() })
	for _, id := range []int{1, 2, 5} {
		createChannelSelectAutoGroupsChannel(t, db, id, "default", "health-model")
	}
	require.NoError(t, db.Model(&model.Channel{}).Where("id IN ?", []int{1, 2}).Update("type", constant.ChannelTypeCodex).Error)
	model.InitChannelCache()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	common.SetContextKey(c, constant.ContextKeyUserGroup, "default")
	retry := 0
	param := &RetryParam{Ctx: c, TokenGroup: "default", ModelName: "health-model", Retry: &retry}
	failed := relaycommon.NewStreamStatus()
	failed.MarkFailed("server_error", "server_error", 500)
	failed.SetEndReason(relaycommon.StreamEndReasonEOF, nil)
	for range 3 {
		RecordCodexStreamHealth(2, failed)
	}
	selected, _, err := CacheGetRandomSatisfiedChannel(param)
	require.NoError(t, err)
	require.Equal(t, 1, selected.Id)
	for range 3 {
		RecordCodexStreamHealth(1, failed)
	}
	selected, _, err = CacheGetRandomSatisfiedChannel(param)
	require.NoError(t, err)
	require.Equal(t, 5, selected.Id)
	require.True(t, IsCodexOverflowFallback(c))
	s.FastForward(codexChannelCooldown)
	selected, _, err = CacheGetRandomSatisfiedChannel(param)
	require.NoError(t, err)
	require.Equal(t, constant.ChannelTypeCodex, selected.Type)
	require.False(t, IsCodexOverflowFallback(c))
}

func TestCodexFailureWindowAndBrokenStream(t *testing.T) {
	s := miniredis.RunT(t)
	prevEnabled, prevRDB := common.RedisEnabled, common.RDB
	client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	common.RedisEnabled, common.RDB = true, client
	t.Cleanup(func() { common.RedisEnabled, common.RDB = prevEnabled, prevRDB; _ = client.Close() })
	status := relaycommon.NewStreamStatus()
	status.RequireTerminal()
	status.SetEndReason(relaycommon.StreamEndReasonScannerErr, errors.New("upstream reset"))
	RecordCodexStreamHealth(2, status)
	s.FastForward(codexFailureWindow)
	for range 2 {
		RecordCodexStreamHealth(2, status)
	}
	require.False(t, IsChannelModelCoolingDown(2, "model"))
	RecordCodexStreamHealth(2, status)
	require.True(t, IsChannelModelCoolingDown(2, "model"))
}

func TestResponsesFailureDiagnosticsPersistWithoutMessages(t *testing.T) {
	for _, payload := range []string{
		`{"type":"error","error":{"code":"rate_limit_exceeded","type":"server_error","status":429,"message":"secret prompt"}}`,
		`{"type":"response.failed","response":{"status":"failed","error":{"code":"rate_limit_exceeded","type":"server_error","status":429,"message":"secret prompt"}}}`,
	} {
		status := relaycommon.NewStreamStatus()
		status.MarkFailed("", "", 0)
		status.SetEndReason(relaycommon.StreamEndReasonEOF, nil)
		info := &relaycommon.RelayInfo{IsStream: true, StreamStatus: status}
		ObserveResponsesErrorPayload(info, payload)
		other := model.NewLogOther()
		appendStreamStatus(info, other)
		data, err := common.Marshal(other)
		require.NoError(t, err)
		require.Contains(t, string(data), `"error_code":"rate_limit_exceeded"`)
		require.Contains(t, string(data), `"error_type":"server_error"`)
		require.Contains(t, string(data), `"error_status":429`)
		require.NotContains(t, string(data), "secret prompt")
	}
	require.Empty(t, responsesErrorLabel("server_error\nsecret"))
}

func TestCodexConcurrentFailuresAndInvalidRequests(t *testing.T) {
	s := miniredis.RunT(t)
	prevEnabled, prevRDB := common.RedisEnabled, common.RDB
	client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	common.RedisEnabled, common.RDB = true, client
	t.Cleanup(func() { common.RedisEnabled, common.RDB = prevEnabled, prevRDB; _ = client.Close() })
	invalid := relaycommon.NewStreamStatus()
	invalid.MarkFailed("invalid_request", "invalid_request_error", 400)
	invalid.SetEndReason(relaycommon.StreamEndReasonEOF, nil)
	for range 4 {
		RecordCodexStreamHealth(2, invalid)
	}
	failureKey, _ := codexChannelHealthKeys(2)
	require.False(t, s.Exists(failureKey))
	failed := relaycommon.NewStreamStatus()
	failed.MarkFailed("server_error", "server_error", 500)
	failed.SetEndReason(relaycommon.StreamEndReasonEOF, nil)
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() { RecordCodexStreamHealth(2, failed) })
	}
	wg.Wait()
	require.True(t, IsChannelModelCoolingDown(2, "any-model"))
	_, cooldown := codexChannelHealthKeys(2)
	require.Equal(t, codexChannelCooldown, s.TTL(cooldown))
}
