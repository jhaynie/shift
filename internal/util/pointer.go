package util

// Ptr will return a pointer to value T
func Ptr[T any](t T) *T {
	return &t
}
