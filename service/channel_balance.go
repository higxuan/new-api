package service

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/model"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
)

const (
	channelBalanceWindow         = 2 * time.Minute
	channelBalanceRetention      = 3 * channelBalanceWindow
	channelBalanceMinimumSamples = int64(10)
	channelAffinityMinimumHold   = 10 * time.Minute
	channelBalanceNamespace      = "new-api:channel_balance:v2"
)

func channelBalanceKey(group, modelName string) string {
	hash := sha256.Sum256([]byte(group + "\x00" + modelName))
	return fmt.Sprintf("%s:{%x}", channelBalanceNamespace, hash[:8])
}

// Redis time aligns windows across gateway instances. Keep the current and
// two completed windows; inactive counters expire rather than growing forever.
var recordChannelBalanceScript = redis.NewScript(`
local now = redis.call('TIME')
local bucket = math.floor(tonumber(now[1]) / tonumber(ARGV[2]))
redis.call('HINCRBY', KEYS[1], bucket .. ':' .. ARGV[1], 1)
for _, field in ipairs(redis.call('HKEYS', KEYS[1])) do
  local window = tonumber(string.match(field, '^(%d+):'))
  if window and window < bucket - 2 then redis.call('HDEL', KEYS[1], field) end
end
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 1
`)

var channelBalanceHistoryScript = redis.NewScript(`
local now = redis.call('TIME')
local bucket = math.floor(tonumber(now[1]) / tonumber(ARGV[1]))
local result = {}
for offset = 0, 2 do
  for i = 2, #ARGV do
    table.insert(result, tonumber(redis.call('HGET', KEYS[1], (bucket - offset) .. ':' .. ARGV[i])) or 0)
  end
end
return result
`)

func recordChannelBalance(group, modelName string, channel *model.Channel) {
	if channel == nil || (channel.Type != constant.ChannelTypeCodex && channel.Type != constant.ChannelTypeOpenAI) || !common.RedisEnabled || common.RDB == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := recordChannelBalanceScript.Run(ctx, common.RDB, []string{channelBalanceKey(group, modelName)}, channel.Id, int(channelBalanceWindow.Seconds()), channelBalanceRetention.Milliseconds()).Err(); err != nil {
		common.SysError(fmt.Sprintf("record channel balance failed: channel=%d", channel.Id))
	}
}

func channelBalanceHistory(group, modelName string, candidates []*model.Channel) [3]map[int]int64 {
	var history [3]map[int]int64
	for i := range history {
		history[i] = make(map[int]int64)
	}
	if !common.RedisEnabled || common.RDB == nil || len(candidates) == 0 {
		return history
	}
	args := []any{int(channelBalanceWindow.Seconds())}
	for _, channel := range candidates {
		args = append(args, channel.Id)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	values, err := channelBalanceHistoryScript.Run(ctx, common.RDB, []string{channelBalanceKey(group, modelName)}, args...).Slice()
	if err != nil {
		return history
	}
	for window := range history {
		for i, channel := range candidates {
			if count := values[window*len(candidates)+i].(int64); count > 0 {
				history[window][channel.Id] = count
			}
		}
	}
	return history
}

func channelBalanceCounts(group, modelName string, candidates []*model.Channel) map[int]int64 {
	return channelBalanceHistory(group, modelName, candidates)[0]
}

// Rebalance only after two completed windows show a sustained, weight-adjusted
// 2:1 imbalance with sufficient samples. OpenAI shares the pool during recovery;
// once Codex recovers, its temporary bindings must return to primary routing.
func ShouldRebalanceChannelAffinity(group, modelName string, preferredID int, filters []dto.ChannelFilter) bool {
	if preferredID <= 0 || strings.TrimSpace(group) == "" || strings.TrimSpace(modelName) == "" || !common.RedisEnabled || common.RDB == nil {
		return false
	}
	preferred, err := model.CacheGetChannel(preferredID)
	if err != nil || preferred == nil || (preferred.Type != constant.ChannelTypeCodex && preferred.Type != constant.ChannelTypeOpenAI) {
		return false
	}
	protection := CodexProtectionActive(group, modelName, filters)
	var candidates []*model.Channel
	for _, id := range model.GetCandidateChannelIDs(group, modelName, filters) {
		channel, err := model.CacheGetChannel(id)
		if err != nil || channel == nil || (channel.Type != constant.ChannelTypeCodex && !(protection && channel.Type == constant.ChannelTypeOpenAI)) || channel.Status != common.ChannelStatusEnabled {
			continue
		}
		if !(preferred.Type == constant.ChannelTypeOpenAI && !protection) && channel.GetPriority() != preferred.GetPriority() {
			continue
		}
		if IsChannelModelCoolingDown(id, modelName) {
			continue
		}
		// New sessions can probe recovering accounts, but never migrate a healthy
		// established session into an account that is still ramping up.
		if channel.Type == constant.ChannelTypeCodex && common.RDB.Exists(context.Background(), codexChannelCircuitKeys(id)[2]).Val() > 0 {
			continue
		}
		candidates = append(candidates, channel)
	}
	if preferred.Type == constant.ChannelTypeOpenAI && !protection {
		return len(candidates) > 0
	}
	if len(candidates) < 2 {
		return false
	}
	history := channelBalanceHistory(group, modelName, candidates)
	for _, candidate := range candidates {
		if candidate.Id == preferredID {
			continue
		}
		sustained := true
		for window := 1; window <= 2; window++ {
			if history[window][preferredID]+history[window][candidate.Id] < channelBalanceMinimumSamples {
				sustained = false
				break
			}
			preferredLoad := float64(history[window][preferredID]) / float64(max(1, preferred.GetWeight()))
			candidateLoad := float64(history[window][candidate.Id]) / float64(max(1, candidate.GetWeight()))
			if preferredLoad == 0 || preferredLoad < 2*candidateLoad {
				sustained = false
				break
			}
		}
		if sustained {
			return true
		}
	}
	return false
}

func recordSelectedChannelBalance(group, modelName string, channel *model.Channel) {
	recordChannelBalance(group, modelName, channel)
}

func channelAffinityHoldKey(cacheKey string) string {
	hash := sha256.Sum256([]byte(cacheKey))
	return fmt.Sprintf("new-api:channel_affinity_hold:v1:{%x}", hash[:16])
}

// Refresh idle expiry without resetting the binding's birth time. A changed
// channel or a cleared binding always starts a new minimum holding period.
var channelAffinityHoldScript = redis.NewScript(`
local now = redis.call('TIME')
local milliseconds = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
if redis.call('HGET', KEYS[1], 'channel') ~= ARGV[1] then
  redis.call('HSET', KEYS[1], 'channel', ARGV[1], 'since', milliseconds)
end
redis.call('PEXPIRE', KEYS[1], ARGV[3])
if milliseconds - tonumber(redis.call('HGET', KEYS[1], 'since')) >= tonumber(ARGV[2]) then return 1 end
return 0
`)

func channelAffinityHoldElapsed(c *gin.Context, channelID int) bool {
	if c == nil || !common.RedisEnabled || common.RDB == nil {
		return false
	}
	cacheKey, ttlSeconds, ok := getChannelAffinityContext(c)
	if !ok {
		return false
	}
	ttl := max(time.Duration(ttlSeconds)*time.Second, channelAffinityMinimumHold)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	elapsed, err := channelAffinityHoldScript.Run(ctx, common.RDB, []string{channelAffinityHoldKey(cacheKey)}, channelID, channelAffinityMinimumHold.Milliseconds(), ttl.Milliseconds()).Int()
	return err == nil && elapsed == 1
}
