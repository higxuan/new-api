package operation_setting

import "github.com/QuantumNous/new-api/setting/config"

// CodexHeaderProfileSetting controls the optional stable client identity sent
// to Codex upstreams. Session and turn headers are never part of this profile.
type CodexHeaderProfileSetting struct {
	Mode                string `json:"mode"` // off, manual, candidate
	CaptureEnabled      bool   `json:"capture_enabled"`
	UserAgent           string `json:"user_agent"`
	Originator          string `json:"originator"`
	OpenAIBeta          string `json:"openai_beta"`
	CodexBetaFeatures   string `json:"codex_beta_features"`
	CandidateTTLSeconds int    `json:"candidate_ttl_seconds"`
}

var codexHeaderProfileSetting = CodexHeaderProfileSetting{
	Mode:                "off",
	CaptureEnabled:      false,
	CandidateTTLSeconds: 7 * 24 * 60 * 60,
}

func init() {
	config.GlobalConfig.Register("codex_header_profile", &codexHeaderProfileSetting)
}

func GetCodexHeaderProfileSetting() *CodexHeaderProfileSetting {
	return &codexHeaderProfileSetting
}
