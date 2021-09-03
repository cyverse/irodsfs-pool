package service

import (
	"fmt"

	"github.com/kelseyhightower/envconfig"
	yaml "gopkg.in/yaml.v2"
)

const (
	ServicePortDefault int = 12020
)

// Config holds the parameters list which can be configured
type Config struct {
	ServicePort int `envconfig:"SERVICE_PORT" yaml:"service_port"`

	LogPath string `envconfig:"LOG_PATH" yaml:"log_path,omitempty"`

	Foreground   bool `yaml:"foreground,omitempty"`
	ChildProcess bool `yaml:"childprocess,omitempty"`
}

// NewDefaultConfig creates DefaultConfig
func NewDefaultConfig() *Config {
	return &Config{
		ServicePort: ServicePortDefault,

		LogPath: "",

		Foreground:   false,
		ChildProcess: false,
	}
}

// NewConfigFromENV creates Config from Environmental Variables
func NewConfigFromENV() (*Config, error) {
	config := Config{
		ServicePort: ServicePortDefault,
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
		ServicePort: ServicePortDefault,
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
