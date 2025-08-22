package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type CarbonIntensityResp struct {
	Data []struct {
		From      string `json:"from"`
		To        string `json:"to"`
		Intensity struct {
			Forecast int    `json:"forecast"`
			Actual   *int   `json:"actual"` // may be null
			Index    string `json:"index"`
		} `json:"intensity"`
	} `json:"data"`
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("missing env %s", key)
	}
	return v
}

func main() {
	// ENV:
	// DB_DSN example: "postgres://energy:energy@localhost:5432/energy?sslmode=disable"
	// LOOKBACK_HOURS optional: default 24
	dsn := mustEnv("DB_DSN")
	lookback := 24
	if os.Getenv("LOOKBACK_HOURS") != "" {
		fmt.Sscanf(os.Getenv("LOOKBACK_HOURS"), "%d", &lookback)
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	defer pool.Close()

	// Time window: now - lookback → now (UTC)
	now := time.Now().UTC().Truncate(30 * time.Minute)
	from := now.Add(-time.Duration(lookback) * time.Hour)

	url := fmt.Sprintf("https://api.carbonintensity.org.uk/intensity/%s/%s",
		from.Format(time.RFC3339), now.Format(time.RFC3339))

	log.Printf("fetching %s", url)
	resp, err := http.Get(url)
	if err != nil {
		log.Fatalf("http get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		log.Fatalf("bad status: %d", resp.StatusCode)
	}

	var payload CarbonIntensityResp
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		log.Fatalf("decode: %v", err)
	}
	if len(payload.Data) == 0 {
		log.Println("no data returned")
		return
	}

	// Upsert rows
	const region = "GB"
	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Fatalf("tx begin: %v", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		} else {
			_ = tx.Commit(ctx)
		}
	}()

	stmt := `
INSERT INTO raw_carbon_intensity (region, from_ts, to_ts, forecast, actual, intensity_index, source)
VALUES ($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT (region, from_ts, to_ts)
DO UPDATE SET forecast = EXCLUDED.forecast,
              actual = EXCLUDED.actual,
              intensity_index = EXCLUDED.intensity_index,
              source = EXCLUDED.source,
              ingested_at = NOW();`

	for _, r := range payload.Data {
		fromTs, err := time.Parse(time.RFC3339, r.From)
		if err != nil {
			log.Fatalf("parse from: %v", err)
		}
		toTs, err := time.Parse(time.RFC3339, r.To)
		if err != nil {
			log.Fatalf("parse to: %v", err)
		}
		var actual *int
		if r.Intensity.Actual != nil {
			actual = r.Intensity.Actual
		}
		_, err = tx.Exec(ctx, stmt,
			region, fromTs, toTs, r.Intensity.Forecast, actual, r.Intensity.Index, "carbon-intensity-api",
		)
		if err != nil {
			log.Fatalf("upsert: %v", err)
		}
	}
	log.Printf("ingestion complete: %d rows", len(payload.Data))
}
