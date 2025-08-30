package main

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	DB_DSN         string
	CarbonAPIURL   string
	OpenAQAPIURL   string
	OpenAQAPIKey   string
	LookbackHours  int
	MaxRetries     int
	TimeoutSeconds int
}

func LoadConfig() Config {
	err := godotenv.Load(".env")
	if err != nil {
		log.Println("No .env file found, reading environment variables")
	}

	lookback := 24
	maxRetries := 3
	timeout := 30

	if v := os.Getenv("LOOKBACK_HOURS"); v != "" {
		fmt.Sscanf(v, "%d", &lookback)
	}
	if v := os.Getenv("MAX_RETRIES"); v != "" {
		fmt.Sscanf(v, "%d", &maxRetries)
	}
	if v := os.Getenv("TIMEOUT_SECONDS"); v != "" {
		fmt.Sscanf(v, "%d", &timeout)
	}

	return Config{
		DB_DSN:         os.Getenv("DB_DSN"),
		CarbonAPIURL:   os.Getenv("CARBON_API_URL"),
		OpenAQAPIURL:   os.Getenv("OPENAQ_API_URL"),
		OpenAQAPIKey:   os.Getenv("OPENAQ_API_KEY"),
		LookbackHours:  lookback,
		MaxRetries:     maxRetries,
		TimeoutSeconds: timeout,
	}
}
