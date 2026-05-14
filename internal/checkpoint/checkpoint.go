package checkpoint

import (
	"encoding/json"
	"errors"
	"os"
)

type Checkpoint struct {
	filename   string
	BinlogFile string `json:"binlog_file"`
	BinlogPos  uint32 `json:"binlog_pos"`
}

func NewCheckpoint(filename, binlogFile string, binlogPos uint32) *Checkpoint {
	return &Checkpoint{
		filename:   filename,
		BinlogFile: binlogFile,
		BinlogPos:  binlogPos,
	}
}

func (c *Checkpoint) Save() error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}

	// save checkpoint to a temp file first, then rename to avoid corruption on crash
	tmp := c.filename + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}

	return os.Rename(tmp, c.filename)
}

func (c *Checkpoint) Update(binlogFile string, binlogPos uint32) error {
	c.BinlogFile = binlogFile
	c.BinlogPos = binlogPos
	return c.Save()
}

func LoadCheckpoint(filename string) (*Checkpoint, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// create checkpoint with default starting position
			cp := &Checkpoint{
				filename:   filename,
				BinlogFile: "mysql-bin.000001",
				BinlogPos:  4,
			}
			if err := cp.Save(); err != nil {
				return nil, err
			}
			return cp, nil
		}
		return nil, err
	}

	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, err
	}
	cp.filename = filename
	return &cp, nil
}
