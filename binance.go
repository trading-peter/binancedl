package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/adshao/go-binance/v2"
)

var tf = map[string]int{
	"1s":  1,
	"1m":  60,
	"3m":  60 * 3,
	"5m":  60 * 5,
	"15m": 60 * 15,
	"30m": 60 * 30,
	"1h":  60 * 60,
	"2h":  60 * 120,
	"4h":  60 * 240,
	"6h":  60 * 60 * 6,
	"8h":  60 * 60 * 8,
	"12h": 60 * 60 * 12,
	"1d":  60 * 60 * 24,
	"3d":  60 * 60 * 24 * 3,
	"1w":  60 * 60 * 24 * 7,
}

func main() {
	ticker := ""
	year := 0
	res := ""

	flag.StringVar(&ticker, "ticker", "", "Ticker to pull data from. For example BTCUSDT.")
	flag.IntVar(&year, "year", 0, "Year to pull.")
	flag.StringVar(&res, "res", "1s", "Resolution to pull.")
	flag.Parse()

	if ticker == "" || year == 0 || res == "" {
		flag.Usage()
		os.Exit(1)
	}

	outFile, _ := os.Create(fmt.Sprintf("./%s_%s_%d.csv", ticker, res, year))
	defer outFile.Close()
	csvWriter := csv.NewWriter(outFile)
	client := binance.NewClient("", "")

	start := time.Date(year, time.January, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Second * time.Duration(tf[res]) * 1000)

	srv := client.NewKlinesService().Symbol(ticker).Interval(res).Limit(1000).StartTime(start.UnixMilli()).EndTime(end.UnixMilli())

	for {
		if start.After(time.Date(year, time.December, 31, 59, 59, 59, 0, time.UTC)) || start.Add(time.Second*time.Duration(tf[res])).After(time.Now().UTC()) {
			break
		}

		fmt.Printf("Downloading data range starting at %s\n", start)
		resp, err := srv.Do(context.Background())

		if err != nil {
			log.Fatalf("Failed to download data: %v", err)
		}

		for i := range resp {
			if time.Unix(resp[i].OpenTime/1000, 0).UTC().Year() > year {
				break
			}

			csvWriter.Write([]string{
				time.Unix(resp[i].OpenTime/1000, 0).UTC().String(),
				resp[i].Open,
				resp[i].High,
				resp[i].Low,
				resp[i].Close,
				resp[i].Volume,
			})
		}

		start = time.Unix(resp[len(resp)-1].OpenTime/1000, 0).UTC()
		end = start.Add(time.Second * time.Duration(tf[res]) * 1000)
		srv.StartTime(start.UnixMilli())
		srv.EndTime(end.UnixMilli())
	}

	csvWriter.Flush()
}
