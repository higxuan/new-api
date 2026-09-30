package service

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/gin-gonic/gin"
)

const codexOverloadCooldown = time.Minute

const codexOverflowFallbackContextKey = "codex_overflow_fallback"

func codexOverloadKey(channelID int, modelName string) string {
	modelHash := sha256.Sum256([]byte(modelName))
	return fmt.Sprintf("new-api:codex_overload:v1:%d:%x", channelID, modelHash[:8])
}

// IsChannelModelCoolingDown fails open when Redis is unavailable. A Redis
// outage must not make every provider look overloaded.
func IsChannelModelCoolingDown(channelID int, modelName string) bool {
	if channelID <= 0 || strings.TrimSpace(modelName) == "" || !common.RedisEnabled || common.RDB == nil {
		return false
	}
	return common.RDB.Exists(context.Background(), codexOverloadKey(channelID, modelName)).Val() > 0
}

func MarkChannelModelOverload(channelID int, modelName string) error {
	if channelID <= 0 || strings.TrimSpace(modelName) == "" || !common.RedisEnabled || common.RDB == nil {
		return nil
	}
	return common.RDB.Set(context.Background(), codexOverloadKey(channelID, modelName), "1", codexOverloadCooldown).Err()
}

// IsUpstreamModelOverload recognizes explicit capacity/overload responses.
// Generic 429 rate limits and generic network failures are intentionally not
// cooled down because they do not prove that this model account is overloaded.
func IsUpstreamModelOverload(err *types.NewAPIError) bool {
	if err == nil || (err.StatusCode != http.StatusTooManyRequests && err.StatusCode != http.StatusServiceUnavailable) {
		return false
	}
	message := err.Error()
	if openAIError := err.ToOpenAIError(); openAIError.Message != "" {
		message += " " + openAIError.Message
	}
	message += " " + string(err.GetErrorCode())
	text := strings.ToLower(message)
	for _, phrase := range []string{
		"model overloaded",
		"model is overloaded",
		"model currently overloaded",
		"overloaded",
		"capacity exceeded",
		"capacity is exceeded",
		"at capacity",
		"load too high",
		"model load",
		"high demand",
	} {
		if strings.Contains(text, phrase) {
			return true
		}
	}
	return false
}

// HandleUpstreamModelOverload records cooling and makes the current retry
// avoid the failed account. It also clears affinity so strict session binding
// cannot prevent migration to another account.
func HandleUpstreamModelOverload(c *gin.Context, retry *RetryParam, channelID int, modelName string, err *types.NewAPIError) bool {
	if !IsUpstreamModelOverload(err) {
		return false
	}
	if markErr := MarkChannelModelOverload(channelID, modelName); markErr != nil {
		common.SysError(fmt.Sprintf("mark codex overload cooldown failed: channel=%d, err=%v", channelID, markErr))
	}
	if retry != nil {
		retry.ExcludeChannel(channelID)
	}
	ClearCurrentChannelAffinityCache(c)
	return true
}

func IsAbnormalCodexStream(status *relaycommon.StreamStatus) bool {
	if status == nil || !status.ResponseFailed() || status.EndReason == relaycommon.StreamEndReasonClientGone {
		return false
	}
	return status.EndReason == relaycommon.StreamEndReasonEOF || status.EndReason == relaycommon.StreamEndReasonScannerErr || status.EndReason == relaycommon.StreamEndReasonTimeout
}

func HandleAbnormalCodexStream(c *gin.Context, status *relaycommon.StreamStatus, channelID int, modelName string) {
	if !IsAbnormalCodexStream(status) {
		return
	}
	if err := MarkChannelModelOverload(channelID, modelName); err != nil {
		common.SysError(fmt.Sprintf("mark codex stream cooldown failed: channel=%d, err=%v", channelID, err))
	}
	ClearCurrentChannelAffinityCache(c)
}

func IsCodexOverflowFallback(c *gin.Context) bool {
	return c != nil && c.GetBool(codexOverflowFallbackContextKey)
}
