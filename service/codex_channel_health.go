package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/go-redis/redis/v8"
)

const (
	codexFailureWindow   = 2 * time.Minute
	codexChannelCooldown = 5 * time.Minute
)

// The first failure starts a fixed window. Later failures do not extend it,
// and in-flight responses cannot extend an already active cooldown.
var codexChannelFailureScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[2]) == 1 then return 0 end
local n = redis.call('INCR', KEYS[1])
if n == 1 then redis.call('PEXPIRE', KEYS[1], ARGV[1]) end
if n >= 3 then
  redis.call('SET', KEYS[2], '1', 'PX', ARGV[2])
  redis.call('DEL', KEYS[1])
  return 1
end
return 0
`)

func codexChannelHealthKeys(channelID int) (string, string) {
	prefix := fmt.Sprintf("new-api:codex_channel_health:v1:{%d}:", channelID)
	return prefix + "failures", prefix + "cooldown"
}

// RecordCodexStreamHealth counts protocol failures and broken upstream streams.
// Client cancellation and normal completed EOF never penalize the account.
func RecordCodexStreamHealth(channelID int, status *relaycommon.StreamStatus) {
	if channelID <= 0 || status == nil || !common.RedisEnabled || common.RDB == nil {
		return
	}
	outcome := status.OutcomeSnapshot()
	if outcome.EndReason == relaycommon.StreamEndReasonClientGone || outcome.Response == relaycommon.ResponseOutcomeCancelled {
		return
	}
	// Validation failures belong to the request, not the account's health.
	if outcome.ErrorType == "invalid_request_error" || strings.HasPrefix(outcome.ErrorCode, "context_length") || outcome.ErrorCode == "invalid_request" {
		return
	}
	failures, cooldown := codexChannelHealthKeys(channelID)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if outcome.Response == relaycommon.ResponseOutcomeCompleted && status.IsNormalEnd() && !outcome.HasErrors {
		// A late successful response clears failures but never an active cooldown.
		if err := common.RDB.Del(ctx, failures).Err(); err != nil {
			common.SysError(fmt.Sprintf("reset codex failures: channel=%d err=%v", channelID, err))
		}
		return
	}
	failed := outcome.Response == relaycommon.ResponseOutcomeFailed ||
		outcome.Response == relaycommon.ResponseOutcomeUnknown && (outcome.EndReason == relaycommon.StreamEndReasonScannerErr || outcome.EndReason == relaycommon.StreamEndReasonTimeout || outcome.ExpectsTerminal && outcome.EndReason == relaycommon.StreamEndReasonEOF)
	if !failed {
		return
	}
	opened, err := codexChannelFailureScript.Run(ctx, common.RDB, []string{failures, cooldown}, codexFailureWindow.Milliseconds(), codexChannelCooldown.Milliseconds()).Int()
	if err != nil {
		common.SysError(fmt.Sprintf("record codex failures: channel=%d err=%v", channelID, err))
	} else if opened == 1 {
		common.SysLog(fmt.Sprintf("codex channel cooling down: channel=%d seconds=%d", channelID, int(codexChannelCooldown.Seconds())))
	}
}
