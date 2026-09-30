package service

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	rootcommon "github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
)

const codexHeaderProfileCandidateKey = "new-api:codex_header_profile:v1:candidate"

var codexUserAgentVersionPattern = regexp.MustCompile(`(?i)codex[^0-9]*([0-9]+(?:\.[0-9]+)+)`)

type codexHeaderProfile struct {
	UserAgent         string `json:"user_agent"`
	Originator        string `json:"originator"`
	OpenAIBeta        string `json:"openai_beta"`
	CodexBetaFeatures string `json:"codex_beta_features"`
	Version           string `json:"version"`
	Fingerprint       string `json:"fingerprint"`
}

func ObserveCodexHeaderProfile(c *gin.Context) {
	setting := operation_setting.GetCodexHeaderProfileSetting()
	if setting == nil || !setting.CaptureEnabled || !rootcommon.RedisEnabled || rootcommon.RDB == nil || c == nil || c.Request == nil {
		return
	}
	profile, ok := codexHeaderProfileFromRequest(c.Request.Header)
	if !ok {
		return
	}
	encoded, err := rootcommon.Marshal(profile)
	if err != nil {
		return
	}
	ttl := codexHeaderProfileTTL(setting)
	ctx := context.Background()
	// WATCH makes the version check and write one logical operation. Without it,
	// two simultaneous requests could let an older client overwrite a newer
	// candidate after both had read the same Redis value.
	_ = rootcommon.RDB.Watch(ctx, func(tx *redis.Tx) error {
		current, err := tx.Get(ctx, codexHeaderProfileCandidateKey).Result()
		if err == nil {
			var existing codexHeaderProfile
			if rootcommon.Unmarshal([]byte(current), &existing) == nil && compareCodexVersions(profile.Version, existing.Version) < 0 {
				return nil
			}
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, codexHeaderProfileCandidateKey, encoded, ttl)
			return nil
		})
		return err
	}, codexHeaderProfileCandidateKey)
}

func ApplyCodexHeaderProfile(c *gin.Context, channelType int, headers *http.Header) {
	if channelType != constant.ChannelTypeCodex || headers == nil {
		return
	}
	ObserveCodexHeaderProfile(c)
	setting := operation_setting.GetCodexHeaderProfileSetting()
	if setting == nil || strings.EqualFold(strings.TrimSpace(setting.Mode), "off") {
		return
	}
	profile, ok := configuredCodexHeaderProfile(setting)
	if !ok {
		return
	}
	if profile.UserAgent != "" {
		headers.Set("User-Agent", profile.UserAgent)
	}
	if profile.Originator != "" {
		headers.Set("Originator", profile.Originator)
	}
	if profile.OpenAIBeta != "" {
		headers.Set("OpenAI-Beta", profile.OpenAIBeta)
	}
	if profile.CodexBetaFeatures != "" {
		headers.Set("X-Codex-Beta-Features", profile.CodexBetaFeatures)
	}
}

func configuredCodexHeaderProfile(setting *operation_setting.CodexHeaderProfileSetting) (codexHeaderProfile, bool) {
	mode := normalizedCodexHeaderProfileMode(setting.Mode)
	if mode == "manual" {
		profile := codexHeaderProfile{UserAgent: cleanHeaderValue(setting.UserAgent), Originator: cleanHeaderValue(setting.Originator), OpenAIBeta: cleanHeaderValue(setting.OpenAIBeta), CodexBetaFeatures: cleanHeaderValue(setting.CodexBetaFeatures)}
		return profile, profile.UserAgent != "" || profile.Originator != "" || profile.OpenAIBeta != "" || profile.CodexBetaFeatures != ""
	}
	if mode != "candidate" || !rootcommon.RedisEnabled || rootcommon.RDB == nil {
		return codexHeaderProfile{}, false
	}
	value, err := rootcommon.RDB.Get(context.Background(), codexHeaderProfileCandidateKey).Result()
	if err != nil {
		return codexHeaderProfile{}, false
	}
	var profile codexHeaderProfile
	if rootcommon.Unmarshal([]byte(value), &profile) != nil {
		return codexHeaderProfile{}, false
	}
	return profile, profile.UserAgent != ""
}

func normalizedCodexHeaderProfileMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "manual", "candidate":
		return strings.ToLower(strings.TrimSpace(mode))
	default:
		return "off"
	}
}

func codexHeaderProfileTTL(setting *operation_setting.CodexHeaderProfileSetting) time.Duration {
	if setting == nil || setting.CandidateTTLSeconds <= 0 {
		return 7 * 24 * time.Hour
	}
	return time.Duration(setting.CandidateTTLSeconds) * time.Second
}

func codexHeaderProfileFromRequest(headers http.Header) (codexHeaderProfile, bool) {
	ua := cleanHeaderValue(headers.Get("User-Agent"))
	originator := cleanHeaderValue(headers.Get("Originator"))
	if ua == "" || (!strings.Contains(strings.ToLower(ua), "codex") && !strings.EqualFold(originator, "codex_cli_rs")) {
		return codexHeaderProfile{}, false
	}
	match := codexUserAgentVersionPattern.FindStringSubmatch(ua)
	if len(match) != 2 {
		return codexHeaderProfile{}, false
	}
	profile := codexHeaderProfile{
		UserAgent:         ua,
		Originator:        originator,
		OpenAIBeta:        cleanHeaderValue(headers.Get("OpenAI-Beta")),
		CodexBetaFeatures: cleanHeaderValue(headers.Get("X-Codex-Beta-Features")),
		Version:           match[1],
	}
	hash := sha256.Sum256([]byte(profile.UserAgent + "\x00" + profile.Originator + "\x00" + profile.OpenAIBeta + "\x00" + profile.CodexBetaFeatures))
	profile.Fingerprint = fmt.Sprintf("%x", hash[:6])
	return profile, true
}

func cleanHeaderValue(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 512 || strings.ContainsAny(value, "\r\n") {
		return ""
	}
	return value
}

func compareCodexVersions(left, right string) int {
	leftParts := parseCodexVersion(left)
	rightParts := parseCodexVersion(right)
	for i := range max(len(leftParts), len(rightParts)) {
		var l, r int
		if i < len(leftParts) {
			l = leftParts[i]
		}
		if i < len(rightParts) {
			r = rightParts[i]
		}
		if l != r {
			if l > r {
				return 1
			}
			return -1
		}
	}
	return 0
}

func parseCodexVersion(version string) []int {
	parts := strings.Split(version, ".")
	values := make([]int, 0, len(parts))
	for _, part := range parts {
		value, err := strconv.Atoi(part)
		if err != nil {
			return nil
		}
		values = append(values, value)
	}
	return values
}
