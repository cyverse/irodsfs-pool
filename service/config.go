package service

import (
	"fmt"

	"github.com/kelseyhightower/envconfig"
	yaml "gopkg.in/yaml.v2"
)

const (
	ServicePortDefault   int   = 12020
	BufferSizeMaxDefault int64 = 1024 * 1024 * 64 // 64MB
)

// Config holds the parameters list which can be configured
type Config struct {
	ServicePort   int   `envconfig:"SERVICE_PORT" yaml:"service_port"`
	BufferSizeMax int64 `envconfig:"BUFFER_SIZE_MAX" yaml:"buffer_size_max"`

	LogPath string `envconfig:"LOG_PATH" yaml:"log_path,omitempty"`

	Foreground   bool `yaml:"foreground,omitempty"`
	ChildProcess bool `yaml:"childprocess,omitempty"`
}

// NewDefaultConfig creates DefaultConfig
func NewDefaultConfig() *Config {
	return &Config{
		ServicePort:   ServicePortDefault,
		BufferSizeMax: BufferSizeMaxDefault,

		LogPath: "",

		Foreground:   false,
		ChildProcess: false,
	}
}

// NewConfigFromENV creates Config from Environmental Variables
func NewConfigFromENV() (*Config, error) {
	config := Config{
		ServicePort:   ServicePortDefault,
		BufferSizeMax: BufferSizeMaxDefault,
	}

	err := envconfig.Process("", &config)
	if err != nil {
		return nil, fmt.Errorf("Env Read Error - %v", err)
	}

	return &config, nil
}

// NewConfigFromYAML creates Config from YAML
func NewConfigFromYAML(yamlBytes []byte) (*Config, error) {
	config := Config{
		ServicePort:   ServicePortDefault,
		BufferSizeMax: BufferSizeMaxDefault,
	}

	err := yaml.Unmarshal(yamlBytes, &config)
	if err != nil {
		return nil, fmt.Errorf("YAML Unmarshal Error - %v", err)
	}

	return &config, nil
}

// Validate validates configuration
func (config *Config) Validate() error {
	if config.ServicePort <= 0 {
		return fmt.Errorf("Service port must be given")
	}

	return nil
}
