package api_fetchers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type OpenAQResp struct {
	Results []struct {
		Location  string  `json:"location"`
		City      string  `json:"city"`
		Country   string  `json:"country"`
		Parameter string  `json:"parameter"`
		Value     float64 `json:"value"`
		Unit      string  `json:"unit"`
		Date      struct {
			UTC string `json:"utc"`
		} `json:"date"`
	} `json:"results"`
}

func FetchOpenAQ(pool *pgxpool.Pool, apiURL, apiKey string, lookbackHours int) {
	ctx := context.Background()
	now := time.Now().UTC()
	from := now.Add(-time.Duration(lookbackHours) * time.Hour)
	url := fmt.Sprintf("%s?date_from=%s&limit=1000", apiURL, from.Format(time.RFC3339))
	log.Printf("Fetching OpenAQ data from: %s\n", url)

	req, _ := http.NewRequest("GET", url, nil)
	if apiKey != "" {
		req.Header.Set("x-api-key", apiKey)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Fatalf("Bad response status: %d", resp.StatusCode)
	}

	var payload OpenAQResp
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		log.Fatalf("JSON decode failed: %v", err)
	}

	// Upsert into raw_openaq table
	stmt := `
INSERT INTO raw_openaq (location, city, country, parameter, value, unit, measured_at, source)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (location, parameter, measured_at)
DO UPDATE SET value = EXCLUDED.value,
              unit = EXCLUDED.unit,
              source = EXCLUDED.source;`

	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	for _, r := range payload.Results {
		measuredAt := r.Date.UTC
		_, err := tx.Exec(ctx, stmt,
			r.Location, r.City, r.Country, r.Parameter, r.Value, r.Unit, measuredAt, "openaq-api",
		)
		if err != nil {
			log.Fatalf("Upsert failed: %v", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		log.Fatalf("Transaction commit failed: %v", err)
	}

	log.Printf("OpenAQ ingestion complete: %d rows\n", len(payload.Results))
}
