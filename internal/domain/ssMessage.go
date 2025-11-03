package domain



type SSMessage struct {
	records []*Record
	walID 	uint64
}

func NewSSMessage(records []*Record, walID uint64) *SSMessage {
	return &SSMessage{
		records: records,
		walID: walID,
	}
}

func (s *SSMessage) GetRecords() []*Record {
	return s.records
}

func (s *SSMessage) GetWalID() uint64 {
	return s.walID
}
