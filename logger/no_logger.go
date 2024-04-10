package logger

type NoLogger struct {
}

func NewNoLogger() Logger {
	return &NoLogger{}
}

func (n *NoLogger) Debug(msg string, args ...Field) {

}

func (n *NoLogger) Info(msg string, args ...Field) {

}

func (n *NoLogger) Warn(msg string, args ...Field) {

}

func (n *NoLogger) Error(msg string, args ...Field) {
}
