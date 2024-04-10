package logger

func Error(err error) Field {
	return Field{
		Key:   "err",
		Value: err,
	}
}

func Any[T any](key string, val T) Field {
	return Field{
		Key:   key,
		Value: val,
	}
}
