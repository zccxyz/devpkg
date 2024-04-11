package events

type InconsistentEvent struct {
	Typ string
	ID  int64
	//src以源表为准 dst以目标表为准
	Direction string
}

const (
	// InconsistentBaseMissing 源表不存在
	InconsistentBaseMissing = "base_missing"
	// InconsistentTargetMissing 目标表不存在
	InconsistentTargetMissing = "target_missing"
	// InconsistentNEQ 不相等
	InconsistentNEQ = "neq"
)
