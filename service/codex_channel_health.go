package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/model"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
)

const (
	codexFailureWindow         = 2 * time.Minute
	codexChannelCooldown       = 5 * time.Minute
	codexMaxChannelCooldown    = 30 * time.Minute
	codexProbeLease            = time.Minute
	codexProbeRenewInterval    = codexProbeLease / 3
	codexRecoveryStageDuration = time.Minute
)

// All circuit keys share a Redis hash slot. Backoff remains until gradual recovery completes;
// expiry of the cooldown alone must never restore unrestricted traffic.
func codexChannelHealthKeys(channelID int) (string, string) {
	prefix := fmt.Sprintf("new-api:codex_channel_health:v1:{%d}:", channelID)
	return prefix + "failures", prefix + "cooldown"
}

func codexChannelCircuitKeys(channelID int) []string {
	failures, cooldown := codexChannelHealthKeys(channelID)
	prefix := strings.TrimSuffix(failures, "failures")
	return []string{failures, cooldown, prefix + "backoff", prefix + "probe", prefix + "ramp", prefix + "gate", prefix + "stage_window"}
}

var codexChannelAdmissionScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[2]) == 1 or redis.call('EXISTS', KEYS[4]) == 1 then return {0, ''} end
if redis.call('EXISTS', KEYS[5]) == 1 then
  if redis.call('EXISTS', KEYS[6]) == 1 then return {0, ''} end
  local stage = tonumber(redis.call('HGET', KEYS[5], 'stage'))
  local intervals = {10000, 5000, 2000}
  redis.call('SET', KEYS[6], '1', 'PX', intervals[stage])
  return {3, redis.call('HGET', KEYS[5], 'id'), stage}
end
if redis.call('EXISTS', KEYS[3]) == 0 then return {1, ''} end
redis.call('SET', KEYS[4], ARGV[1], 'PX', ARGV[2])
return {2, ARGV[1]}
`)

// Only the current probe can start recovery. Recovery admissions are spaced
// and stages advance only after both a minimum duration and successful calls.
// Ordinary successes never erase failures in the fixed window.
var codexChannelResultScript = redis.NewScript(`
local token, outcome, ramp = ARGV[1], tonumber(ARGV[2]), ARGV[6]
if token ~= '' then
  if redis.call('GET', KEYS[4]) ~= token then return 0 end
  redis.call('DEL', KEYS[4])
  if outcome == 1 then
    redis.call('HSET', KEYS[5], 'stage', 1, 'successes', 0, 'id', token)
    redis.call('SET', KEYS[6], '1', 'PX', 10000)
    redis.call('SET', KEYS[7], '1', 'PX', ARGV[7])
    redis.call('DEL', KEYS[1], KEYS[2])
    return -1
  end
  if outcome == 0 then return 0 end
else
  if ramp ~= '' then
    if redis.call('HGET', KEYS[5], 'id') ~= ramp then return 0 end
    if outcome == 1 then
      if tonumber(redis.call('HGET', KEYS[5], 'stage')) ~= tonumber(ARGV[8]) then return 0 end
      local n = redis.call('HINCRBY', KEYS[5], 'successes', 1)
      if n >= 3 and redis.call('EXISTS', KEYS[7]) == 0 and redis.call('EXISTS', KEYS[1]) == 0 then
        local stage = tonumber(redis.call('HGET', KEYS[5], 'stage'))
        if stage == 3 then
          redis.call('DEL', unpack(KEYS))
          return -4
        end
        redis.call('HSET', KEYS[5], 'stage', stage + 1, 'successes', 0)
        local intervals = {10000, 5000, 2000}
        redis.call('SET', KEYS[6], '1', 'PX', intervals[stage + 1])
        redis.call('SET', KEYS[7], '1', 'PX', ARGV[7])
        return -(stage + 1)
      end
      return 0
    end
    if outcome == 0 then return 0 end
  elseif outcome < 2 or redis.call('EXISTS', KEYS[2]) == 1 or redis.call('EXISTS', KEYS[3]) == 1 then
    return 0
  end
  if outcome == 2 then
    local n = redis.call('INCR', KEYS[1])
    if n == 1 then redis.call('PEXPIRE', KEYS[1], ARGV[3]) end
    if n < 3 then return 0 end
  end
end
local level = math.min(4, (tonumber(redis.call('GET', KEYS[3])) or 0) + 1)
local duration = math.min(tonumber(ARGV[5]), tonumber(ARGV[4]) * 2 ^ (level - 1))
redis.call('SET', KEYS[3], level)
redis.call('SET', KEYS[2], '1', 'PX', duration)
redis.call('DEL', KEYS[1], KEYS[5], KEYS[6], KEYS[7])
return duration
`)

var codexProbeLeaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if tonumber(ARGV[2]) == 0 then return redis.call('DEL', KEYS[1]) end
return redis.call('PEXPIRE', KEYS[1], ARGV[2])
`)

// CodexChannelAttempt owns admission for one upstream attempt, not a session.
// Only recovery probes need a lease; healthy channels retain normal routing.
type CodexChannelAttempt struct {
	channelID       int
	keys            []string
	token           string
	rampID          string
	rampStage       int
	requestContext  context.Context
	client          *redis.Client
	c               *gin.Context
	originalRequest *http.Request
	cancel          context.CancelFunc
	stop            chan struct{}
	done            chan struct{}
	once            sync.Once
}

// BeginCodexChannelAttempt performs atomic admission immediately before sending
// upstream. Redis failure preserves the existing fail-open routing policy.
func BeginCodexChannelAttempt(c *gin.Context, channelID int) (*CodexChannelAttempt, bool) {
	if channelID <= 0 || !common.RedisEnabled || common.RDB == nil {
		return nil, true
	}
	a := &CodexChannelAttempt{channelID: channelID, keys: codexChannelCircuitKeys(channelID), client: common.RDB, c: c}
	token := common.GetUUID()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	result, err := codexChannelAdmissionScript.Run(ctx, a.client, a.keys, token, codexProbeLease.Milliseconds()).Slice()
	cancel()
	if err != nil {
		common.SysError(fmt.Sprintf("codex channel admission failed: channel=%d", channelID))
		return nil, true
	}
	admitted := result[0].(int64)
	if admitted == 0 {
		return nil, false
	}
	if admitted == 1 {
		return a, true
	}
	if admitted == 3 {
		a.rampID = result[1].(string)
		a.rampStage = int(result[2].(int64))
		return a, true
	}
	a.token = token
	a.originalRequest = c.Request
	requestContext, requestCancel := context.WithCancel(c.Request.Context())
	a.cancel = requestCancel
	a.requestContext = requestContext
	c.Request = c.Request.WithContext(requestContext)
	a.stop, a.done = make(chan struct{}), make(chan struct{})
	go a.renewProbeLease()
	common.SysLog(fmt.Sprintf("codex channel recovery probe: channel=%d", channelID))
	return a, true
}

func (a *CodexChannelAttempt) renewProbeLease() {
	defer close(a.done)
	ticker := time.NewTicker(codexProbeRenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-a.stop:
			return
		case <-a.requestContext.Done():
			return
		case <-ticker.C:
			if !a.renewProbe() {
				return
			}
		}
	}
}

func (a *CodexChannelAttempt) renewProbe() bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	renewed, err := codexProbeLeaseScript.Run(ctx, a.client, []string{a.keys[3]}, a.token, codexProbeLease.Milliseconds()).Int()
	if err == nil && renewed == 1 {
		return true
	}
	// Cancel the old upstream before its lease can expire and another probe starts.
	a.cancel()
	common.SysError(fmt.Sprintf("codex recovery probe lease lost: channel=%d", a.channelID))
	return false
}

// Finish is called after the upstream attempt (including the entire stream).
func (a *CodexChannelAttempt) Finish(status *relaycommon.StreamStatus, apiErr *types.NewAPIError) {
	if a == nil {
		return
	}
	a.once.Do(func() {
		a.stopRenewal()
		outcome := codexChannelResult(status, apiErr)
		if a.c.Request.Context().Err() != nil {
			outcome = 0
		}
		recordCodexChannelResult(a.client, a.channelID, a.keys, a.token, a.rampID, a.rampStage, outcome)
		a.restoreRequest()
	})
}

// Close releases an abandoned/cancelled probe without treating it as recovery.
// Defer it so preparation errors and panics cannot leave a live lease behind.
func (a *CodexChannelAttempt) Close() {
	if a == nil {
		return
	}
	a.once.Do(func() {
		a.stopRenewal()
		if a.token != "" {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = codexProbeLeaseScript.Run(ctx, a.client, []string{a.keys[3]}, a.token, 0).Err()
		}
		a.restoreRequest()
	})
}

func (a *CodexChannelAttempt) stopRenewal() {
	if a.stop != nil {
		close(a.stop)
		<-a.done
	}
}

func (a *CodexChannelAttempt) restoreRequest() {
	if a.cancel != nil {
		a.cancel()
		a.c.Request = a.originalRequest
	}
}

// 0 = inconclusive/client error, 1 = success, 2 = upstream failure, 3 = overload.
func codexChannelResult(status *relaycommon.StreamStatus, apiErr *types.NewAPIError) int {
	if apiErr != nil && errors.Is(apiErr, context.Canceled) {
		return 0
	}
	if status != nil {
		outcome := status.OutcomeSnapshot()
		if outcome.EndReason == relaycommon.StreamEndReasonClientGone || outcome.Response == relaycommon.ResponseOutcomeCancelled {
			return 0
		}
		if outcome.ErrorType == "invalid_request_error" || strings.HasPrefix(outcome.ErrorCode, "context_length") || outcome.ErrorCode == "invalid_request" {
			return 0
		}
		if outcome.ErrorCode == "server_is_overloaded" || IsUpstreamModelOverload(apiErr) {
			return 3
		}
		if outcome.ErrorStatus >= 400 && outcome.ErrorStatus < 500 && outcome.ErrorStatus != 429 {
			return 0
		}
		if outcome.Response == relaycommon.ResponseOutcomeFailed || outcome.Response == relaycommon.ResponseOutcomeUnknown && (outcome.EndReason == relaycommon.StreamEndReasonScannerErr || outcome.EndReason == relaycommon.StreamEndReasonTimeout || outcome.ExpectsTerminal && outcome.EndReason == relaycommon.StreamEndReasonEOF) {
			return 2
		}
		if apiErr == nil && outcome.Response == relaycommon.ResponseOutcomeCompleted && status.IsNormalEnd() && !outcome.HasErrors {
			return 1
		}
	}
	if IsUpstreamModelOverload(apiErr) {
		return 3
	}
	if apiErr != nil {
		if apiErr.StatusCode >= 500 {
			return 2
		}
		return 0
	}
	if status == nil {
		return 1
	}
	return 0
}

// Used by explicit HTTP overload handling and stream-health regression tests.
func RecordCodexStreamHealth(channelID int, status *relaycommon.StreamStatus) {
	if status == nil || channelID <= 0 || !common.RedisEnabled || common.RDB == nil {
		return
	}
	recordCodexChannelResult(common.RDB, channelID, codexChannelCircuitKeys(channelID), "", "", 0, codexChannelResult(status, nil))
}

func recordCodexChannelResult(client *redis.Client, channelID int, keys []string, token, rampID string, rampStage, outcome int) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	duration, err := codexChannelResultScript.Run(ctx, client, keys, token, outcome, codexFailureWindow.Milliseconds(), codexChannelCooldown.Milliseconds(), codexMaxChannelCooldown.Milliseconds(), rampID, codexRecoveryStageDuration.Milliseconds(), rampStage).Int()
	if err != nil {
		common.SysError(fmt.Sprintf("record codex channel health failed: channel=%d", channelID))
	} else if duration > 0 {
		common.SysLog(fmt.Sprintf("codex channel cooling down: channel=%d seconds=%d", channelID, duration/1000))
	} else if duration == -4 {
		common.SysLog(fmt.Sprintf("codex channel recovered: channel=%d", channelID))
	} else if duration < 0 {
		common.SysLog(fmt.Sprintf("codex channel recovering: channel=%d stage=%d", channelID, -duration))
	}
}

// CodexProtectionActive keeps OpenAI in the routing pool until every configured
// Codex account is enabled and finishes recovery. It never changes channel status.
func CodexProtectionActive(group, modelName string, filters []dto.ChannelFilter) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, id := range model.GetConfiguredCodexChannelIDs(group, modelName, filters) {
		channel, err := model.CacheGetChannel(id)
		if err != nil {
			continue
		}
		if channel.Status != common.ChannelStatusEnabled {
			return true
		}
		if !common.RedisEnabled || common.RDB == nil {
			continue
		}
		keys := codexChannelCircuitKeys(id)
		if common.RDB.Exists(ctx, keys[1], keys[2], codexOverloadKey(id, modelName)).Val() > 0 {
			return true
		}
	}
	return false
}
