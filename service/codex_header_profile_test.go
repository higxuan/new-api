package service

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCodexHeaderProfileFromRequestRequiresVersionedCodexUA(t *testing.T) {
	profile, ok := codexHeaderProfileFromRequest(http.Header{
		"User-Agent":            []string{"codex_cli_rs/0.42.1 (macOS)"},
		"Originator":            []string{"codex_cli_rs"},
		"OpenAI-Beta":           []string{"responses=experimental"},
		"X-Codex-Beta-Features": []string{"feature-a"},
	})
	require.True(t, ok)
	assert.Equal(t, "0.42.1", profile.Version)
	assert.NotEmpty(t, profile.Fingerprint)

	_, ok = codexHeaderProfileFromRequest(http.Header{"User-Agent": []string{"curl/8.0"}})
	assert.False(t, ok)
}

func TestObserveCodexHeaderProfileDoesNotDowngradeCandidate(t *testing.T) {
	server := miniredis.RunT(t)
	previousEnabled, previousRedis := common.RedisEnabled, common.RDB
	previousSetting := *operation_setting.GetCodexHeaderProfileSetting()
	common.RedisEnabled = true
	common.RDB = redis.NewClient(&redis.Options{Addr: server.Addr()})
	setting := operation_setting.GetCodexHeaderProfileSetting()
	setting.CaptureEnabled = true
	setting.CandidateTTLSeconds = 60
	t.Cleanup(func() {
		common.RedisEnabled, common.RDB = previousEnabled, previousRedis
		*setting = previousSetting
	})

	first := testCodexHeaderProfileContext(t, "codex_cli_rs/0.43.0")
	ObserveCodexHeaderProfile(first)
	second := testCodexHeaderProfileContext(t, "codex_cli_rs/0.42.9")
	ObserveCodexHeaderProfile(second)

	value, err := common.RDB.Get(first.Request.Context(), codexHeaderProfileCandidateKey).Result()
	require.NoError(t, err)
	var profile codexHeaderProfile
	require.NoError(t, common.Unmarshal([]byte(value), &profile))
	assert.Equal(t, "0.43.0", profile.Version)
	assert.Greater(t, server.TTL(codexHeaderProfileCandidateKey), time.Duration(0))
}

func TestApplyCodexHeaderProfilePreservesRequestScopedHeaders(t *testing.T) {
	previousEnabled, previousRedis := common.RedisEnabled, common.RDB
	previousSetting := *operation_setting.GetCodexHeaderProfileSetting()
	common.RedisEnabled = false
	setting := operation_setting.GetCodexHeaderProfileSetting()
	setting.Mode = "manual"
	setting.UserAgent = "codex_cli_rs/0.43.0"
	setting.Originator = "codex_cli_rs"
	setting.OpenAIBeta = "responses=experimental"
	setting.CodexBetaFeatures = "feature-a"
	t.Cleanup(func() {
		common.RedisEnabled, common.RDB = previousEnabled, previousRedis
		*setting = previousSetting
	})

	headers := http.Header{
		"User-Agent":         []string{"old-client"},
		"Session_id":         []string{"session-123"},
		"Thread_id":          []string{"thread-123"},
		"X-Codex-Turn-State": []string{"turn-state"},
	}
	ApplyCodexHeaderProfile(nil, constant.ChannelTypeCodex, &headers)
	assert.Equal(t, "codex_cli_rs/0.43.0", headers.Get("User-Agent"))
	assert.Equal(t, "codex_cli_rs", headers.Get("Originator"))
	assert.Equal(t, "session-123", headers.Get("Session_id"))
	assert.Equal(t, "thread-123", headers.Get("Thread_id"))
	assert.Equal(t, "turn-state", headers.Get("X-Codex-Turn-State"))
}

func TestApplyCodexHeaderProfileIgnoresInvalidMode(t *testing.T) {
	previousSetting := *operation_setting.GetCodexHeaderProfileSetting()
	setting := operation_setting.GetCodexHeaderProfileSetting()
	setting.Mode = "unexpected"
	setting.UserAgent = "codex_cli_rs/0.43.0"
	t.Cleanup(func() { *setting = previousSetting })

	headers := http.Header{"User-Agent": []string{"request-client"}}
	ApplyCodexHeaderProfile(nil, constant.ChannelTypeCodex, &headers)
	assert.Equal(t, "request-client", headers.Get("User-Agent"))
}

func testCodexHeaderProfileContext(t *testing.T, userAgent string) *gin.Context {
	t.Helper()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Request.Header.Set("User-Agent", userAgent)
	c.Request.Header.Set("Originator", "codex_cli_rs")
	return c
}
