package models

type CDCEvent struct {
	Database  string                 `json:"database"`
	Table     string                 `json:"table"`
	Operation string                 `json:"operation"`
	Timestamp uint32                 `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
	OldData   map[string]interface{} `json:"old_data,omitempty"`
}

type CDCMessage struct {
	Event      CDCEvent
	BinlogFile string
	BinlogPos  uint32
}
