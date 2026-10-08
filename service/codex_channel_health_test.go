package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/pkg/cachex"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
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
	RecordCodexStreamHealth(1, successful) // successes must not erase the window
	RecordCodexStreamHealth(1, failed)
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
	assert.Contains(t, []int{1, 5}, selected.Id)
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
	assert.Contains(t, []int{1, 2, 5}, selected.Id)
	assert.True(t, CodexProtectionActive("default", "health-model", nil))
	assert.Equal(t, selected.Type == constant.ChannelTypeOpenAI, IsCodexOverflowFallback(c))
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

func setupCodexRecoveryTest(t *testing.T) *miniredis.Miniredis {
	t.Helper()
	s := miniredis.RunT(t)
	previousEnabled, previousRedis := common.RedisEnabled, common.RDB
	client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	common.RedisEnabled, common.RDB = true, client
	t.Cleanup(func() { common.RedisEnabled, common.RDB = previousEnabled, previousRedis; _ = client.Close() })
	return s
}

func codexRecoveryContext() *gin.Context {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	return c
}

func codexRecoveryOutcome(code string) *relaycommon.StreamStatus {
	status := relaycommon.NewStreamStatus()
	if code == "" {
		status.MarkCompleted()
	} else {
		status.MarkFailed(code, "server_error", 500)
	}
	status.SetEndReason(relaycommon.StreamEndReasonEOF, nil)
	return status
}

func TestCodexExplicitOverloadAndProgressiveBackoff(t *testing.T) {
	s := setupCodexRecoveryTest(t)
	c := codexRecoveryContext()
	attempt, admitted := BeginCodexChannelAttempt(c, 2)
	require.True(t, admitted)
	attempt.Finish(nil, types.NewErrorWithStatusCode(errors.New("model is overloaded"), types.ErrorCodeBadResponseStatusCode, 503))
	assert.True(t, IsChannelModelCoolingDown(2, "any-model"))
	assert.False(t, IsChannelModelCoolingDown(1, "any-model"))
	for _, duration := range []time.Duration{5 * time.Minute, 10 * time.Minute, 20 * time.Minute, 30 * time.Minute, 30 * time.Minute} {
		s.FastForward(duration - time.Second)
		blocked, allowed := BeginCodexChannelAttempt(codexRecoveryContext(), 2)
		assert.False(t, allowed)
		blocked.Close()
		s.FastForward(time.Second)
		probe, allowed := BeginCodexChannelAttempt(codexRecoveryContext(), 2)
		require.True(t, allowed)
		require.NotEmpty(t, probe.token)
		second, allowed := BeginCodexChannelAttempt(codexRecoveryContext(), 2)
		assert.False(t, allowed)
		second.Close()
		probe.Finish(codexRecoveryOutcome("server_is_overloaded"), nil)
	}
}

func TestCodexRecoveryProbeCancellationRenewalAndOwnership(t *testing.T) {
	s := setupCodexRecoveryTest(t)
	RecordCodexStreamHealth(2, codexRecoveryOutcome("server_is_overloaded"))
	s.FastForward(codexChannelCooldown)
	c := codexRecoveryContext()
	original := c.Request
	probe, admitted := BeginCodexChannelAttempt(c, 2)
	require.True(t, admitted)
	child := c.Request.Context()
	s.FastForward(45 * time.Second)
	require.True(t, probe.renewProbe())
	s.FastForward(45 * time.Second)
	blocked, allowed := BeginCodexChannelAttempt(codexRecoveryContext(), 2)
	assert.False(t, allowed)
	blocked.Close()
	probe.Close()
	assert.Same(t, original, c.Request)
	assert.ErrorIs(t, child.Err(), context.Canceled)
	// Cancellation is not recovery and does not increase the cooldown.
	next, allowed := BeginCodexChannelAttempt(codexRecoveryContext(), 2)
	require.True(t, allowed)
	require.NotEmpty(t, next.token)
	s.FastForward(codexProbeLease)
	replacement, allowed := BeginCodexChannelAttempt(codexRecoveryContext(), 2)
	require.True(t, allowed)
	require.NotEmpty(t, replacement.token)
	// An expired owner must neither release the replacement nor recover it.
	next.Finish(codexRecoveryOutcome(""), nil)
	blocked, allowed = BeginCodexChannelAttempt(codexRecoveryContext(), 2)
	assert.False(t, allowed)
	blocked.Close()
	replacement.Close()
}

func TestCodexRecoveryProbeLeaseLossCancelsUpstream(t *testing.T) {
	s := setupCodexRecoveryTest(t)
	RecordCodexStreamHealth(2, codexRecoveryOutcome("server_is_overloaded"))
	s.FastForward(codexChannelCooldown)
	c := codexRecoveryContext()
	probe, admitted := BeginCodexChannelAttempt(c, 2)
	require.True(t, admitted)
	defer probe.Close()
	s.FastForward(codexProbeLease)
	assert.False(t, probe.renewProbe())
	assert.ErrorIs(t, c.Request.Context().Err(), context.Canceled)
	probe.Finish(codexRecoveryOutcome(""), nil)
	retry, allowed := BeginCodexChannelAttempt(codexRecoveryContext(), 2)
	require.True(t, allowed)
	defer retry.Close()
	assert.NotEmpty(t, retry.token)
}

func TestCodexRecoveryProbeAdmissionIsAtomic(t *testing.T) {
	s := setupCodexRecoveryTest(t)
	RecordCodexStreamHealth(2, codexRecoveryOutcome("server_is_overloaded"))
	s.FastForward(codexChannelCooldown)
	admitted := make(chan *CodexChannelAttempt, 8)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			attempt, allowed := BeginCodexChannelAttempt(codexRecoveryContext(), 2)
			if allowed {
				admitted <- attempt
			}
		})
	}
	wg.Wait()
	close(admitted)
	var probes []*CodexChannelAttempt
	for probe := range admitted {
		probes = append(probes, probe)
		defer probe.Close()
	}
	assert.Len(t, probes, 1)
}

func TestCodexGradualRecoverySpacingAndStableStages(t *testing.T) {
	s := setupCodexRecoveryTest(t)
	RecordCodexStreamHealth(2, codexRecoveryOutcome("server_is_overloaded"))
	s.FastForward(codexChannelCooldown)
	probe, admitted := BeginCodexChannelAttempt(codexRecoveryContext(), 2)
	require.True(t, admitted)
	probe.Finish(codexRecoveryOutcome(""), nil)
	for _, interval := range []time.Duration{10 * time.Second, 5 * time.Second, 2 * time.Second} {
		// Three quick successes cannot skip the minimum stable stage duration.
		for range 3 {
			blocked, allowed := BeginCodexChannelAttempt(codexRecoveryContext(), 2)
			assert.False(t, allowed)
			blocked.Close()
			s.FastForward(interval)
			attempt, allowed := BeginCodexChannelAttempt(codexRecoveryContext(), 2)
			require.True(t, allowed)
			require.NotEmpty(t, attempt.rampID)
			attempt.Finish(codexRecoveryOutcome(""), nil)
		}
		s.FastForward(time.Minute)
		attempt, allowed := BeginCodexChannelAttempt(codexRecoveryContext(), 2)
		require.True(t, allowed)
		require.NotEmpty(t, attempt.rampID)
		attempt.Finish(codexRecoveryOutcome(""), nil)
	}
	// After all stages, simultaneous admissions are unrestricted again.
	for range 2 {
		attempt, allowed := BeginCodexChannelAttempt(codexRecoveryContext(), 2)
		require.True(t, allowed)
		assert.Empty(t, attempt.token)
		assert.Empty(t, attempt.rampID)
		attempt.Close()
	}
	// Full recovery resets the backoff for a new incident.
	RecordCodexStreamHealth(2, codexRecoveryOutcome("server_is_overloaded"))
	s.FastForward(codexChannelCooldown)
	next, allowed := BeginCodexChannelAttempt(codexRecoveryContext(), 2)
	require.True(t, allowed)
	next.Close()
}

func TestCodexGradualRecoveryFailureReturnsToCooldown(t *testing.T) {
	s := setupCodexRecoveryTest(t)
	RecordCodexStreamHealth(2, codexRecoveryOutcome("server_is_overloaded"))
	s.FastForward(codexChannelCooldown)
	probe, admitted := BeginCodexChannelAttempt(codexRecoveryContext(), 2)
	require.True(t, admitted)
	probe.Finish(codexRecoveryOutcome(""), nil)
	s.FastForward(10 * time.Second)
	early, allowed := BeginCodexChannelAttempt(codexRecoveryContext(), 2)
	require.True(t, allowed)
	s.FastForward(10 * time.Second)
	overloaded, allowed := BeginCodexChannelAttempt(codexRecoveryContext(), 2)
	require.True(t, allowed)
	overloaded.Finish(codexRecoveryOutcome("server_is_overloaded"), nil)
	early.Finish(codexRecoveryOutcome(""), nil) // late success cannot reopen the account
	s.FastForward(10*time.Minute - time.Second)
	blocked, allowed := BeginCodexChannelAttempt(codexRecoveryContext(), 2)
	assert.False(t, allowed)
	blocked.Close()
	s.FastForward(time.Second)
	probe, allowed = BeginCodexChannelAttempt(codexRecoveryContext(), 2)
	require.True(t, allowed)
	assert.NotEmpty(t, probe.token)
	probe.Close()
}

func finishCodexGradualRecovery(t *testing.T, s *miniredis.Miniredis, id int) {
	t.Helper()
	probe, admitted := BeginCodexChannelAttempt(codexRecoveryContext(), id)
	require.True(t, admitted)
	require.NotEmpty(t, probe.token)
	probe.Finish(codexRecoveryOutcome(""), nil)
	for _, interval := range []time.Duration{10 * time.Second, 5 * time.Second, 2 * time.Second} {
		s.FastForward(time.Minute)
		for range 3 {
			s.FastForward(interval)
			attempt, allowed := BeginCodexChannelAttempt(codexRecoveryContext(), id)
			require.True(t, allowed)
			require.NotEmpty(t, attempt.rampID)
			attempt.Finish(codexRecoveryOutcome(""), nil)
		}
	}
}

func TestCodexProtectionSharesOpenAIUntilAllAccountsRecover(t *testing.T) {
	db := setupChannelSelectAutoGroupsTest(t)
	s := setupCodexRecoveryTest(t)
	for _, id := range []int{1, 2, 5} {
		createChannelSelectAutoGroupsChannel(t, db, id, "default", "recovery-model")
	}
	require.NoError(t, db.Model(&model.Channel{}).Where("id IN ?", []int{1, 2}).Update("type", constant.ChannelTypeCodex).Error)
	// Higher OpenAI priority makes participation deterministic without random samples.
	require.NoError(t, db.Model(&model.Channel{}).Where("id = ?", 5).Update("priority", 1).Error)
	model.InitChannelCache()
	c := codexRecoveryContext()
	common.SetContextKey(c, constant.ContextKeyUserGroup, "default")
	param := &RetryParam{Ctx: c, TokenGroup: "default", ModelName: "recovery-model", Retry: common.GetPointer(0)}
	selected, _, err := CacheGetRandomSatisfiedChannel(param)
	require.NoError(t, err)
	assert.Equal(t, constant.ChannelTypeCodex, selected.Type)
	RecordCodexStreamHealth(2, codexRecoveryOutcome("server_is_overloaded"))
	assert.True(t, CodexProtectionActive("default", "recovery-model", nil))
	assert.False(t, CodexProtectionActive("other-group", "recovery-model", nil))
	assert.False(t, CodexProtectionActive("default", "other-model", nil))
	selected, _, err = CacheGetRandomSatisfiedChannel(param)
	require.NoError(t, err)
	assert.Equal(t, 5, selected.Id)            // channel 1 is healthy, yet OpenAI shares traffic
	assert.True(t, IsCodexOverflowFallback(c)) // do not create sticky OpenAI bindings
	for range 3 {
		recordChannelBalance("default", "recovery-model", &model.Channel{Id: 1, Type: constant.ChannelTypeCodex})
	}
	assert.False(t, ShouldRebalanceChannelAffinity("default", "recovery-model", 1, nil)) // a short spike cannot migrate a sticky session
	s.FastForward(codexChannelCooldown)
	RecordCodexStreamHealth(1, codexRecoveryOutcome("server_is_overloaded"))
	finishCodexGradualRecovery(t, s, 2)
	assert.True(t, CodexProtectionActive("default", "recovery-model", nil)) // channel 1 still needs recovery
	selected, _, err = CacheGetRandomSatisfiedChannel(param)
	require.NoError(t, err)
	assert.Equal(t, 5, selected.Id)
	s.FastForward(codexChannelCooldown)
	finishCodexGradualRecovery(t, s, 1)
	assert.False(t, CodexProtectionActive("default", "recovery-model", nil))
	selected, _, err = CacheGetRandomSatisfiedChannel(param)
	require.NoError(t, err)
	assert.Equal(t, constant.ChannelTypeCodex, selected.Type)
	assert.False(t, IsCodexOverflowFallback(c))
	assert.True(t, ShouldRebalanceChannelAffinity("default", "recovery-model", 5, nil))
}

func TestCodexProtectionPreservesManualDisabling(t *testing.T) {
	db := setupChannelSelectAutoGroupsTest(t)
	setupCodexRecoveryTest(t)
	for _, id := range []int{1, 2, 5} {
		createChannelSelectAutoGroupsChannel(t, db, id, "default", "recovery-model")
	}
	require.NoError(t, db.Model(&model.Channel{}).Where("id IN ?", []int{1, 2}).Update("type", constant.ChannelTypeCodex).Error)
	require.NoError(t, db.Model(&model.Channel{}).Where("id = ?", 5).Update("priority", 1).Error)
	model.InitChannelCache()
	require.NoError(t, db.Model(&model.Channel{}).Where("id = ?", 2).Update("status", common.ChannelStatusManuallyDisabled).Error)
	model.InitChannelCache()
	assert.True(t, CodexProtectionActive("default", "recovery-model", nil))
	c := codexRecoveryContext()
	common.SetContextKey(c, constant.ContextKeyUserGroup, "default")
	selected, _, err := CacheGetRandomSatisfiedChannel(&RetryParam{Ctx: c, TokenGroup: "default", ModelName: "recovery-model"})
	require.NoError(t, err)
	assert.Equal(t, 5, selected.Id)
	disabled, err := model.CacheGetChannel(2)
	require.NoError(t, err)
	assert.Equal(t, common.ChannelStatusManuallyDisabled, disabled.Status)
}

func TestCodexRecoveryGenericFailuresAccumulateAcrossSuccesses(t *testing.T) {
	s := setupCodexRecoveryTest(t)
	RecordCodexStreamHealth(2, codexRecoveryOutcome("server_is_overloaded"))
	s.FastForward(codexChannelCooldown)
	probe, admitted := BeginCodexChannelAttempt(codexRecoveryContext(), 2)
	require.True(t, admitted)
	probe.Finish(codexRecoveryOutcome(""), nil)
	for _, code := range []string{"server_error", "", "server_error", "", "server_error"} {
		s.FastForward(10 * time.Second)
		attempt, allowed := BeginCodexChannelAttempt(codexRecoveryContext(), 2)
		require.True(t, allowed)
		attempt.Finish(codexRecoveryOutcome(code), nil)
	}
	s.FastForward(10*time.Minute - time.Second)
	blocked, allowed := BeginCodexChannelAttempt(codexRecoveryContext(), 2)
	assert.False(t, allowed)
	blocked.Close()
	s.FastForward(time.Second)
	next, allowed := BeginCodexChannelAttempt(codexRecoveryContext(), 2)
	require.True(t, allowed)
	assert.NotEmpty(t, next.token)
	next.Close()
}

func TestCodexOldRecoveryStageCannotAdvanceNewStage(t *testing.T) {
	s := setupCodexRecoveryTest(t)
	RecordCodexStreamHealth(2, codexRecoveryOutcome("server_is_overloaded"))
	s.FastForward(codexChannelCooldown)
	probe, admitted := BeginCodexChannelAttempt(codexRecoveryContext(), 2)
	require.True(t, admitted)
	probe.Finish(codexRecoveryOutcome(""), nil)
	s.FastForward(time.Minute)
	var old *CodexChannelAttempt
	for i := range 4 {
		s.FastForward(10 * time.Second)
		attempt, allowed := BeginCodexChannelAttempt(codexRecoveryContext(), 2)
		require.True(t, allowed)
		if i == 0 {
			old = attempt
		} else {
			attempt.Finish(codexRecoveryOutcome(""), nil)
		}
	}
	old.Finish(codexRecoveryOutcome(""), nil)
	// One new-stage success plus an old-stage success must not satisfy three successes.
	s.FastForward(time.Minute)
	for range 2 {
		s.FastForward(5 * time.Second)
		attempt, allowed := BeginCodexChannelAttempt(codexRecoveryContext(), 2)
		require.True(t, allowed)
		assert.Equal(t, 2, attempt.rampStage)
		attempt.Finish(codexRecoveryOutcome(""), nil)
	}
	s.FastForward(5 * time.Second)
	third, allowed := BeginCodexChannelAttempt(codexRecoveryContext(), 2)
	require.True(t, allowed)
	assert.Equal(t, 2, third.rampStage)
	third.Close()
}

func TestChannelAffinityBalanceRequiresCompletedSustainedWeightedWindows(t *testing.T) {
	for _, tc := range []struct {
		name    string
		weights [2]uint
		windows [3][2]int
		want    bool
	}{
		{name: "sustained imbalance", weights: [2]uint{1, 1}, windows: [3][2]int{{8, 2}, {8, 2}, {0, 0}}, want: true},
		{name: "one bad window", weights: [2]uint{1, 1}, windows: [3][2]int{{8, 2}, {5, 5}, {0, 0}}},
		{name: "small sample", weights: [2]uint{1, 1}, windows: [3][2]int{{8, 1}, {8, 1}, {0, 0}}},
		{name: "configured two to one is normal", weights: [2]uint{4, 2}, windows: [3][2]int{{8, 4}, {8, 4}, {0, 0}}},
		{name: "imbalance beyond configured weights", weights: [2]uint{4, 2}, windows: [3][2]int{{10, 2}, {10, 2}, {0, 0}}, want: true},
		{name: "current spike is not confirmation", weights: [2]uint{1, 1}, windows: [3][2]int{{5, 5}, {5, 5}, {20, 0}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := setupChannelSelectAutoGroupsTest(t)
			s := setupCodexRecoveryTest(t)
			for i, id := range []int{1, 2} {
				createChannelSelectAutoGroupsChannel(t, db, id, "default", "sticky-model")
				require.NoError(t, db.Model(&model.Channel{}).Where("id = ?", id).Updates(map[string]any{"type": constant.ChannelTypeCodex, "weight": tc.weights[i]}).Error)
			}
			model.InitChannelCache()
			base := time.Unix(120000, 0)
			for window, counts := range tc.windows {
				s.SetTime(base.Add(time.Duration(window) * channelBalanceWindow))
				for i, id := range []int{1, 2} {
					for range counts[i] {
						recordChannelBalance("default", "sticky-model", &model.Channel{Id: id, Type: constant.ChannelTypeCodex})
					}
				}
			}
			assert.Equal(t, tc.want, ShouldRebalanceChannelAffinity("default", "sticky-model", 1, nil))
		})
	}
}

func TestChannelAffinityMinimumHoldDoesNotSlideAndResetsOnMigration(t *testing.T) {
	s := setupCodexRecoveryTest(t)
	base := time.Unix(120000, 0)
	s.SetTime(base)
	c := codexRecoveryContext()
	setChannelAffinityContext(c, channelAffinityMeta{CacheKey: "hold-test", TTLSeconds: 3600})
	assert.False(t, channelAffinityHoldElapsed(c, 1))
	s.SetTime(base.Add(channelAffinityMinimumHold - time.Second))
	assert.False(t, channelAffinityHoldElapsed(c, 1))
	s.SetTime(base.Add(channelAffinityMinimumHold))
	assert.True(t, channelAffinityHoldElapsed(c, 1))
	assert.False(t, channelAffinityHoldElapsed(c, 2))
	s.SetTime(base.Add(2 * channelAffinityMinimumHold))
	assert.True(t, channelAffinityHoldElapsed(c, 2))
	// Another session cannot inherit the first session's mature binding.
	other := codexRecoveryContext()
	setChannelAffinityContext(other, channelAffinityMeta{CacheKey: "another-hold-test", TTLSeconds: 3600})
	assert.False(t, channelAffinityHoldElapsed(other, 2))
}

func TestChannelAffinitySelectionPreservesYoungBindingsAndOverridesOnOverload(t *testing.T) {
	for _, overloaded := range []bool{false, true} {
		t.Run(fmt.Sprintf("overloaded=%t", overloaded), func(t *testing.T) {
			originalCache := getChannelAffinityCache()
			db := setupChannelSelectAutoGroupsTest(t)
			s := setupCodexRecoveryTest(t)
			channelAffinityCache = cachex.NewHybridCache[int](cachex.HybridCacheConfig[int]{Namespace: cachex.Namespace(channelAffinityCacheNamespace), Redis: common.RDB, RedisEnabled: func() bool { return true }, RedisCodec: cachex.IntCodec{}})
			t.Cleanup(func() { channelAffinityCache = originalCache })
			setting := operation_setting.GetChannelAffinitySetting()
			previousEnabled, previousRules := setting.Enabled, setting.Rules
			setting.Enabled = true
			setting.Rules = []operation_setting.ChannelAffinityRule{{Name: "sticky-test", ModelRegex: []string{"^sticky-model$"}, PathRegex: []string{"/v1/responses"}, TTLSeconds: 3600, IncludeRuleName: true, IncludeModelName: true, IncludeUsingGroup: true, KeySources: []operation_setting.ChannelAffinityKeySource{{Type: "request_header", Key: "X-Affinity-Test"}}}}
			t.Cleanup(func() { setting.Enabled, setting.Rules = previousEnabled, previousRules })
			for _, id := range []int{1, 2} {
				createChannelSelectAutoGroupsChannel(t, db, id, "default", "sticky-model")
			}
			require.NoError(t, db.Model(&model.Channel{}).Where("id IN ?", []int{1, 2}).Update("type", constant.ChannelTypeCodex).Error)
			model.InitChannelCache()
			base := time.Unix(120000, 0)
			s.SetTime(base)
			c := codexRecoveryContext()
			c.Request.Header.Set("X-Affinity-Test", t.Name())
			common.SetContextKey(c, constant.ContextKeyUserGroup, "default")
			common.SetContextKey(c, constant.ContextKeyChannelId, 1)
			RequestPolicy(c).SessionMode = "prefer"
			_, _ = GetPreferredChannelByAffinity(c, "sticky-model", "default")
			RecordChannelAffinity(c, 1)
			for _, minute := range []int{2, 4} {
				s.SetTime(base.Add(time.Duration(minute) * time.Minute))
				for range 10 {
					recordChannelBalance("default", "sticky-model", &model.Channel{Id: 1, Type: constant.ChannelTypeCodex})
				}
			}
			s.SetTime(base.Add(6 * time.Minute))
			require.True(t, ShouldRebalanceChannelAffinity("default", "sticky-model", 1, nil))
			param := &RetryParam{Ctx: c, TokenGroup: "default", ModelName: "sticky-model", Retry: common.GetPointer(0)}
			selected, _, selectErr := SelectChannelForRequest(c, "sticky-model", param)
			require.Nil(t, selectErr)
			assert.Equal(t, 1, selected.Id) // sustained pressure alone cannot break the ten-minute hold
			if overloaded {
				RecordCodexStreamHealth(1, codexRecoveryOutcome("server_is_overloaded"))
			} else {
				for _, minute := range []int{6, 8} {
					s.SetTime(base.Add(time.Duration(minute) * time.Minute))
					for range 10 {
						recordChannelBalance("default", "sticky-model", &model.Channel{Id: 1, Type: constant.ChannelTypeCodex})
					}
				}
				s.SetTime(base.Add(channelAffinityMinimumHold))
			}
			selected, _, selectErr = SelectChannelForRequest(c, "sticky-model", param)
			require.Nil(t, selectErr)
			assert.Equal(t, 2, selected.Id)
			common.SetContextKey(c, constant.ContextKeyChannelId, selected.Id)
			RecordChannelAffinity(c, selected.Id)
			assert.False(t, channelAffinityHoldElapsed(c, selected.Id))
		})
	}
}
