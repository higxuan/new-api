package service

import (
	"context"
	"errors"
	"github.com/QuantumNous/new-api/constant"
	"github.com/gin-gonic/gin"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsUpstreamModelOverloadOnlyMatchesExplicitCapacityErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int
		text string
		want bool
	}{
		{name: "model overload", code: http.StatusServiceUnavailable, text: "model is overloaded", want: true},
		{name: "capacity in relay error", code: http.StatusTooManyRequests, text: "capacity exceeded", want: true},
		{name: "ordinary rate limit", code: http.StatusTooManyRequests, text: "rate limit exceeded", want: false},
		{name: "ordinary network error", code: http.StatusBadGateway, text: "connection reset", want: false},
		{name: "cancelled request", code: http.StatusServiceUnavailable, text: "context canceled", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := types.NewErrorWithStatusCode(errors.New(tc.text), types.ErrorCodeBadResponseStatusCode, tc.code)
			assert.Equal(t, tc.want, IsUpstreamModelOverload(err))
		})
	}
}

func TestMarkChannelModelOverloadUsesShortRedisTTL(t *testing.T) {
	server := miniredis.RunT(t)
	previousEnabled, previousRedis := common.RedisEnabled, common.RDB
	common.RedisEnabled = true
	common.RDB = redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		common.RedisEnabled = previousEnabled
		common.RDB = previousRedis
	})

	require.NoError(t, MarkChannelModelOverload(42, "gpt-6-sol"))
	assert.True(t, IsChannelModelCoolingDown(42, "gpt-6-sol"))
	assert.False(t, IsChannelModelCoolingDown(43, "gpt-6-sol"))
	server.FastForward(codexOverloadCooldown)
	assert.False(t, IsChannelModelCoolingDown(42, "gpt-6-sol"))
}

func TestCodexOverloadRetrySafety(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		written, cancelled, skip bool
		want                     string
	}{
		{name: "before response", want: "retry"},
		{name: "response started", written: true, want: "stop"},
		{name: "cancelled", cancelled: true, want: "stop"},
		{name: "skip retry", skip: true, want: "stop"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			c.Set("channel_type", constant.ChannelTypeCodex)
			c.Set(ginKeyChannelAffinitySkipRetry, true)
			if tc.written {
				c.Writer.WriteHeaderNow()
			}
			if tc.cancelled {
				ctx, cancel := context.WithCancel(c.Request.Context())
				cancel()
				c.Request = c.Request.WithContext(ctx)
			}
			err := types.NewOpenAIError(errors.New("model is overloaded"), types.ErrorCodeBadResponseStatusCode, 503)
			if tc.skip {
				types.ErrOptionWithSkipRetry()(err)
			}
			assert.Equal(t, tc.want, DecideRelayRetry(c, err, 2).Action)
		})
	}
}

func TestAbnormalCodexStreamClassification(t *testing.T) {
	failedEOF := relaycommon.NewStreamStatus()
	failedEOF.RequireTerminal()
	failedEOF.MarkFailed("incomplete", "upstream", 0)
	failedEOF.SetEndReason(relaycommon.StreamEndReasonEOF, nil)
	assert.True(t, IsAbnormalCodexStream(failedEOF))

	clientGone := relaycommon.NewStreamStatus()
	clientGone.RequireTerminal()
	clientGone.MarkFailed("cancelled", "client", 0)
	clientGone.SetEndReason(relaycommon.StreamEndReasonClientGone, context.Canceled)
	assert.False(t, IsAbnormalCodexStream(clientGone))
}

func TestChannelBalanceCountersExpireAndRemainPerChannel(t *testing.T) {
	server := miniredis.RunT(t)
	previousEnabled, previousRedis := common.RedisEnabled, common.RDB
	common.RedisEnabled = true
	common.RDB = redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		common.RedisEnabled = previousEnabled
		common.RDB = previousRedis
	})

	recordChannelBalance("default", "gpt-6-sol", &model.Channel{Id: 1, Type: constant.ChannelTypeCodex})
	recordChannelBalance("default", "gpt-6-sol", &model.Channel{Id: 1, Type: constant.ChannelTypeCodex})
	recordChannelBalance("default", "gpt-6-sol", &model.Channel{Id: 2, Type: constant.ChannelTypeCodex})
	counts := channelBalanceCounts("default", "gpt-6-sol", []*model.Channel{{Id: 1}, {Id: 2}})
	assert.Equal(t, int64(2), counts[1])
	assert.Equal(t, int64(1), counts[2])
	server.FastForward(channelBalanceRetention)
	assert.Empty(t, channelBalanceCounts("default", "gpt-6-sol", []*model.Channel{{Id: 1}, {Id: 2}}))
}
