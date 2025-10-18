package config

import "os"

type Config struct {
	Port            string
	HiggsfieldAPIKey string
	Environment     string
}

func Load() *Config {
	return &Config{
		Port:            getEnv("PORT", "8080"),
		HiggsfieldAPIKey: getEnv("HIGGSFIELD_API_KEY", ""),
		Environment:     getEnv("ENV", "development"),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}