package domain

type Operation byte

const (
	OP_PUT = Operation(0)
	OP_DELETE = Operation(1)
)