package utils

import (
	"crypto/rand"
	"encoding/binary"
)

func GenerateID() uint64 {
	rawID := make([]byte, 8)
	rand.Read(rawID)
	id := binary.LittleEndian.Uint64(rawID)

	return id
}