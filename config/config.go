package config

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Compactor  CompactorConfig  `yaml:"compactor"`
	SSTManager SSTManagerConfig `yaml:"sstmanager"`
	WAL        WalConfig        `yaml:"wal"`
	SSTWorkdir string           `yaml:"sstWorkdir"`
	SSTPrefix  string           `yaml:"sstPrefix"`
	Memtable   MemtableConfig   `yaml:"memtable"`
}

type CompactorConfig struct {
	TableSize int `yaml:"tableSize"`
	LevelSize int `yaml:"levelSize"`
}

type SSTManagerConfig struct {
	Bloom     BloomConfig `yaml:"bloom"`
	FlushSize int         `yaml:"flushSize"`
	ClearTime int		  `yaml:"clearTime"`
}

type WalConfig struct {
	WalWorkdir string `yaml:"walWorkdir"`
	WALPrefix  string `yaml:"walPrefix"`
}

type MemtableConfig struct {
	MaxSize int `yaml:"maxSize"`
}

type BloomConfig struct {
	ExpectedElements  uint64  `yaml:"expectedElements"`
	FalsePositiveRate float64 `yaml:"falsePositiveRate"`
}

func LoadConfig() (*Config, error) {

	err := godotenv.Load()
	if err != nil {
		return nil, fmt.Errorf(" .env file not found")
	}

	cfgDir := os.Getenv("CONFIG_DIR")
	if cfgDir == "" {
		return nil, fmt.Errorf("empty config_dir env")
	}

	data, err := os.ReadFile(cfgDir)
	if err != nil {
		return nil, err
	}

	var cfg Config
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, err
	}
	log.Println("cfg - ", cfg)
	return &cfg, nil
}
