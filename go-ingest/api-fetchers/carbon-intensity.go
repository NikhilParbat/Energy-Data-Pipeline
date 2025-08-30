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

type CarbonIntensityResp struct {
	Data []struct {
		From      string `json:"from"`
		To        string `json:"to"`
		Intensity struct {
			Forecast int    `json:"forecast"`
			Actual   *int   `json:"actual"`
			Index    string `json:"index"`
		} `json:"intensity"`
	} `json:"data"`
}

func FetchCarbonIntensity(pool *pgxpool.Pool, apiURL string, lookbackHours int) {
	ctx := context.Background() // ✅ define context here

	now := time.Now().UTC().Truncate(30 * time.Minute)
	from := now.Add(-time.Duration(lookbackHours) * time.Hour)

	url := fmt.Sprintf("%s/%s/%s", apiURL, from.Format(time.RFC3339), now.Format(time.RFC3339))
	log.Printf("Fetching Carbon Intensity data from: %s\n", url)

	resp, err := http.Get(url)
	if err != nil {
		log.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Fatalf("Bad response status: %d", resp.StatusCode)
	}

	var payload CarbonIntensityResp
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		log.Fatalf("JSON decode failed: %v", err)
	}

	const region = "GB"
	stmt := `
INSERT INTO raw_carbon_intensity (region, from_ts, to_ts, forecast, actual, intensity_index, source)
VALUES ($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT (region, from_ts, to_ts)
DO UPDATE SET forecast = EXCLUDED.forecast,
              actual = EXCLUDED.actual,
              intensity_index = EXCLUDED.intensity_index,
              source = EXCLUDED.source,
              ingested_at = NOW();`

	tx, err := pool.Begin(ctx) // ✅ pass ctx instead of pool.Context()
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx) // ✅ pass ctx

	for _, r := range payload.Data {
		fromTs, toTs := r.From, r.To
		_, err := tx.Exec(ctx, stmt,
			region,
			fromTs,
			toTs,
			r.Intensity.Forecast,
			r.Intensity.Actual, // can pass directly, nil-safe
			r.Intensity.Index,
			"carbon-intensity-api",
		)
		if err != nil {
			log.Fatalf("Upsert failed: %v", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		log.Fatalf("Transaction commit failed: %v", err)
	}

	log.Printf("Carbon Intensity ingestion complete: %d rows\n", len(payload.Data))
}
