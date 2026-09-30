package service

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/model"
)

const (
	channelBalanceWindow    = 2 * time.Minute
	channelBalanceSkew      = int64(3)
	channelBalanceNamespace = "new-api:channel_balance:v1"
)

func channelBalanceKey(group, modelName string, channelID int) string {
	hash := sha256.Sum256([]byte(group + "\x00" + modelName))
	return fmt.Sprintf("%s:%x:%d", channelBalanceNamespace, hash[:8], channelID)
}

func recordChannelBalance(group, modelName string, channel *model.Channel) {
	if channel == nil || channel.Type != constant.ChannelTypeCodex || !common.RedisEnabled || common.RDB == nil {
		return
	}
	ctx := context.Background()
	key := channelBalanceKey(group, modelName, channel.Id)
	pipe := common.RDB.TxPipeline()
	pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, channelBalanceWindow)
	if _, err := pipe.Exec(ctx); err != nil {
		common.SysError(fmt.Sprintf("record channel balance failed: channel=%d, err=%v", channel.Id, err))
	}
}

func channelBalanceCounts(group, modelName string, candidates []*model.Channel) map[int]int64 {
	counts := make(map[int]int64, len(candidates))
	if !common.RedisEnabled || common.RDB == nil || len(candidates) == 0 {
		return counts
	}
	keys := make([]string, 0, len(candidates))
	for _, channel := range candidates {
		keys = append(keys, channelBalanceKey(group, modelName, channel.Id))
	}
	values, err := common.RDB.MGet(context.Background(), keys...).Result()
	if err != nil {
		return counts
	}
	for i, value := range values {
		if value == nil {
			continue
		}
		if count, err := strconv.ParseInt(fmt.Sprint(value), 10, 64); err == nil {
			counts[candidates[i].Id] = count
		}
	}
	return counts
}

// ShouldRebalanceChannelAffinity keeps normal session stickiness, but releases
// a hot Codex binding when another healthy Codex account is materially cooler.
// Redis failures fail open and preserve the existing affinity behavior.
func ShouldRebalanceChannelAffinity(group, modelName string, preferredID int, filters []dto.ChannelFilter) bool {
	if preferredID <= 0 || strings.TrimSpace(group) == "" || strings.TrimSpace(modelName) == "" || !common.RedisEnabled || common.RDB == nil {
		return false
	}
	preferred, err := model.CacheGetChannel(preferredID)
	if err != nil || preferred == nil || preferred.Type != constant.ChannelTypeCodex {
		return false
	}
	ids := model.GetCandidateChannelIDs(group, modelName, filters)
	candidates := make([]*model.Channel, 0, len(ids))
	for _, id := range ids {
		channel, err := model.CacheGetChannel(id)
		if err != nil || channel == nil || channel.Type != constant.ChannelTypeCodex || channel.Status != common.ChannelStatusEnabled {
			continue
		}
		if IsChannelModelCoolingDown(id, modelName) {
			continue
		}
		candidates = append(candidates, channel)
	}
	if len(candidates) < 2 {
		return false
	}
	counts := channelBalanceCounts(group, modelName, candidates)
	preferredCount := counts[preferredID]
	minimum := preferredCount
	for _, channel := range candidates {
		minimum = min(minimum, counts[channel.Id])
	}
	return preferredCount >= minimum+channelBalanceSkew
}

func recordSelectedChannelBalance(group, modelName string, channel *model.Channel) {
	recordChannelBalance(group, modelName, channel)
}
