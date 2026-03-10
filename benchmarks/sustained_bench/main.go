// Sustained load benchmark for Arc ingestion
// Usage: go run benchmarks/sustained_bench/main.go [flags]
//
// Examples:
//   go run benchmarks/sustained_bench/main.go --duration 30
//   go run benchmarks/sustained_bench/main.go --duration 60 --workers 200 --compress zstd
//   go run benchmarks/sustained_bench/main.go --batch-size 5000 --compress gzip
//   go run benchmarks/sustained_bench/main.go --protocol lineprotocol --workers 20
//   go run benchmarks/sustained_bench/main.go --target clickhouse --data-type iot --duration 60
//   go run benchmarks/sustained_bench/main.go --target clickhouse --data-type iot --batch-size 100000 --workers 10
//   go run benchmarks/sustained_bench/main.go --target clickhouse --data-type iot --async-insert --workers 50

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/klauspost/compress/zstd"
)

type Config struct {
	Duration    int
	Workers     int
	BatchSize   int
	Pregenerate int
	Compress    string
	ZstdLevel   int
	DataType    string
	Protocol    string // "msgpack" or "lineprotocol"
	Target      string // "arc" or "clickhouse"
	Host        string
	Port        int
	CHPort      int
	Database    string
	AsyncInsert bool
	Token       string
	TLS         bool
}

type Stats struct {
	totalSent   atomic.Int64
	totalErrors atomic.Int64
	running     atomic.Bool
	// Per-worker latency slices to avoid mutex contention during test
	workerLatencies [][]float64
	latencyMu       sync.Mutex // Only used at end for merging
}

func (s *Stats) initWorkers(n int) {
	s.workerLatencies = make([][]float64, n)
	for i := range s.workerLatencies {
		// Pre-allocate capacity to reduce allocations during test
		s.workerLatencies[i] = make([]float64, 0, 10000)
	}
}

func (s *Stats) addLatency(workerID int, ms float64) {
	// Lock-free append to per-worker slice
	s.workerLatencies[workerID] = append(s.workerLatencies[workerID], ms)
}

func (s *Stats) getPercentile(p float64) float64 {
	// Merge all worker latencies (only called at end of test)
	var total int
	for _, wl := range s.workerLatencies {
		total += len(wl)
	}

	if total == 0 {
		return 0
	}

	all := make([]float64, 0, total)
	for _, wl := range s.workerLatencies {
		all = append(all, wl...)
	}
	sort.Float64s(all)

	idx := int(float64(len(all)) * p)
	if idx >= len(all) {
		idx = len(all) - 1
	}
	return all[idx]
}

// roundTo rounds a float to n decimal places, producing realistic sensor values.
func roundTo(v float64, decimals int) float64 {
	pow := math.Pow(10, float64(decimals))
	return math.Round(v*pow) / pow
}

func generateIOTBatches(count, batchSize int, compress string, zstdLevel int) [][]byte {
	measurements := []string{"cpu", "mem", "disk", "net"}
	hosts := make([]string, 1000)
	for i := range hosts {
		hosts[i] = fmt.Sprintf("server%03d", i)
	}

	batches := make([][]byte, count)
	var zstdEncoder *zstd.Encoder
	if compress == "zstd" {
		var err error
		zstdEncoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(zstdLevel)))
		if err != nil {
			panic(err)
		}
	}

	for i := 0; i < count; i++ {
		nowMicros := time.Now().UnixMicro()

		times := make([]int64, batchSize)
		hostVals := make([]string, batchSize)
		values := make([]float64, batchSize)
		cpuIdle := make([]float64, batchSize)
		cpuUser := make([]float64, batchSize)

		for j := 0; j < batchSize; j++ {
			times[j] = nowMicros + int64(j)
			hostVals[j] = hosts[rand.Intn(len(hosts))]
			values[j] = roundTo(rand.Float64()*100, 2)
			cpuIdle[j] = roundTo(rand.Float64()*100, 2)
			cpuUser[j] = roundTo(rand.Float64()*100, 2)
		}

		payload := map[string]interface{}{
			"m": measurements[rand.Intn(len(measurements))],
			"columns": map[string]interface{}{
				"time":     times,
				"host":     hostVals,
				"value":    values,
				"cpu_idle": cpuIdle,
				"cpu_user": cpuUser,
			},
		}

		data, err := msgpack.Marshal(payload)
		if err != nil {
			panic(err)
		}

		switch compress {
		case "gzip":
			var buf bytes.Buffer
			w, _ := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
			w.Write(data)
			w.Close()
			data = buf.Bytes()
		case "zstd":
			data = zstdEncoder.EncodeAll(data, nil)
		}

		batches[i] = data

		if (i+1)%100 == 0 {
			fmt.Printf("  Progress: %d/%d\n", i+1, count)
		}
	}

	return batches
}

func generateLineProtocolBatches(count, batchSize int, compress string, zstdLevel int, dataType string) [][]byte {
	batches := make([][]byte, count)
	var zstdEncoder *zstd.Encoder
	if compress == "zstd" {
		var err error
		zstdEncoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(zstdLevel)))
		if err != nil {
			panic(err)
		}
	}

	// IOT hosts or financial symbols
	var tags []string
	var measurement string
	if dataType == "financial" {
		measurement = "trades"
		tags = []string{
			"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "TSLA", "JPM", "V", "JNJ",
			"WMT", "PG", "UNH", "HD", "MA", "DIS", "PYPL", "BAC", "ADBE", "NFLX",
		}
	} else {
		measurement = "cpu"
		tags = make([]string, 1000)
		for i := range tags {
			tags[i] = fmt.Sprintf("server%03d", i)
		}
	}

	for i := 0; i < count; i++ {
		nowMicros := time.Now().UnixMicro()
		var buf bytes.Buffer

		for j := 0; j < batchSize; j++ {
			ts := nowMicros + int64(j)
			tag := tags[rand.Intn(len(tags))]

			if dataType == "financial" {
				exchanges := []string{"NYSE", "NASDAQ", "ARCA", "BATS", "IEX"}
				exchange := exchanges[rand.Intn(len(exchanges))]
				basePrice := 10 + rand.Float64()*490
				bid := basePrice - rand.Float64()*0.04 - 0.01
				ask := basePrice + rand.Float64()*0.04 + 0.01
				bidSize := 100 + rand.Intn(9900)
				askSize := 100 + rand.Intn(9900)
				volume := 1 + rand.Intn(999)
				tradeID := 1000000 + rand.Intn(8999999)

				fmt.Fprintf(&buf, "%s,symbol=%s,exchange=%s price=%.4f,bid=%.4f,ask=%.4f,bid_size=%di,ask_size=%di,volume=%di,trade_id=%di %d\n",
					measurement, tag, exchange, basePrice, bid, ask, bidSize, askSize, volume, tradeID, ts*1000)
			} else {
				value := rand.Float64() * 100
				cpuIdle := rand.Float64() * 100
				cpuUser := rand.Float64() * 100

				fmt.Fprintf(&buf, "%s,host=%s value=%.4f,cpu_idle=%.4f,cpu_user=%.4f %d\n",
					measurement, tag, value, cpuIdle, cpuUser, ts*1000)
			}
		}

		data := buf.Bytes()

		switch compress {
		case "gzip":
			var compBuf bytes.Buffer
			w, _ := gzip.NewWriterLevel(&compBuf, gzip.BestSpeed)
			w.Write(data)
			w.Close()
			data = compBuf.Bytes()
		case "zstd":
			data = zstdEncoder.EncodeAll(data, nil)
		}

		batches[i] = data

		if (i+1)%100 == 0 {
			fmt.Printf("  Progress: %d/%d\n", i+1, count)
		}
	}

	return batches
}

func generateRacingBatches(count, batchSize int, compress string, zstdLevel int) [][]byte {
	// F1/Motorsport telemetry - high-frequency car data
	carNumbers := []string{"1", "4", "11", "14", "16", "22", "44", "55", "63", "77",
		"10", "18", "20", "23", "24", "27", "31", "40", "81", "2"}
	drivers := []string{"VER", "NOR", "PER", "ALO", "LEC", "TSU", "HAM", "SAI", "RUS", "BOT",
		"GAS", "STR", "MAG", "HUL", "RIC", "ALB", "SAR", "ZHO", "PIA", "LAW"}

	batches := make([][]byte, count)
	var zstdEncoder *zstd.Encoder
	if compress == "zstd" {
		var err error
		zstdEncoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(zstdLevel)))
		if err != nil {
			panic(err)
		}
	}

	for i := 0; i < count; i++ {
		nowMicros := time.Now().UnixMicro()

		times := make([]int64, batchSize)
		carNumberVals := make([]string, batchSize)
		driverVals := make([]string, batchSize)
		speed := make([]float64, batchSize)    // km/h
		engineRPM := make([]int, batchSize)    // RPM
		throttle := make([]float64, batchSize) // % 0-100
		brake := make([]float64, batchSize)    // % 0-100
		steering := make([]float64, batchSize) // degrees -180 to 180
		gear := make([]int, batchSize)         // 0-8

		for j := 0; j < batchSize; j++ {
			times[j] = nowMicros + int64(j)
			idx := rand.Intn(len(carNumbers))
			carNumberVals[j] = carNumbers[idx]
			driverVals[j] = drivers[idx]
			speed[j] = roundTo(50+rand.Float64()*300, 1)      // 50-350 km/h
			engineRPM[j] = 8000 + rand.Intn(7000)             // 8000-15000 RPM
			throttle[j] = roundTo(rand.Float64()*100, 1)      // 0-100%
			brake[j] = roundTo(rand.Float64()*100, 1)         // 0-100%
			steering[j] = roundTo(-180+rand.Float64()*360, 1) // -180 to 180 degrees
			gear[j] = 1 + rand.Intn(8)                        // 1-8
		}

		payload := map[string]interface{}{
			"m": "car_telemetry",
			"columns": map[string]interface{}{
				"time":       times,
				"car_number": carNumberVals,
				"driver":     driverVals,
				"speed":      speed,
				"engine_rpm": engineRPM,
				"throttle":   throttle,
				"brake":      brake,
				"steering":   steering,
				"gear":       gear,
			},
		}

		data, err := msgpack.Marshal(payload)
		if err != nil {
			panic(err)
		}

		switch compress {
		case "gzip":
			var buf bytes.Buffer
			w, _ := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
			w.Write(data)
			w.Close()
			data = buf.Bytes()
		case "zstd":
			data = zstdEncoder.EncodeAll(data, nil)
		}

		batches[i] = data

		if (i+1)%100 == 0 {
			fmt.Printf("  Progress: %d/%d\n", i+1, count)
		}
	}

	return batches
}

func generateEnergyBatches(count, batchSize int, compress string, zstdLevel int) [][]byte {
	// Wind turbine telemetry - energy/renewables sector
	turbineIDs := make([]string, 200)
	for i := range turbineIDs {
		turbineIDs[i] = fmt.Sprintf("turbine_%03d", i)
	}
	farms := []string{"north_ridge", "coastal_1", "plains_west", "offshore_a", "hilltop"}

	batches := make([][]byte, count)
	var zstdEncoder *zstd.Encoder
	if compress == "zstd" {
		var err error
		zstdEncoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(zstdLevel)))
		if err != nil {
			panic(err)
		}
	}

	for i := 0; i < count; i++ {
		nowMicros := time.Now().UnixMicro()

		times := make([]int64, batchSize)
		turbineIDVals := make([]string, batchSize)
		farmVals := make([]string, batchSize)
		windSpeed := make([]float64, batchSize)     // m/s
		windDirection := make([]float64, batchSize) // degrees
		rotorRPM := make([]float64, batchSize)      // RPM
		powerOutput := make([]float64, batchSize)   // kW
		bladePitch := make([]float64, batchSize)    // degrees
		nacelleTemp := make([]float64, batchSize)   // Celsius
		generatorTemp := make([]float64, batchSize) // Celsius

		for j := 0; j < batchSize; j++ {
			times[j] = nowMicros + int64(j)
			turbineIDVals[j] = turbineIDs[rand.Intn(len(turbineIDs))]
			farmVals[j] = farms[rand.Intn(len(farms))]
			windSpeed[j] = roundTo(2+rand.Float64()*23, 1)      // 2-25 m/s
			windDirection[j] = roundTo(rand.Float64()*360, 1)   // 0-360 degrees
			rotorRPM[j] = roundTo(5+rand.Float64()*15, 1)       // 5-20 RPM
			powerOutput[j] = roundTo(rand.Float64()*5000, 1)    // 0-5000 kW (5MW turbine)
			bladePitch[j] = roundTo(rand.Float64()*25, 1)       // 0-25 degrees
			nacelleTemp[j] = roundTo(15+rand.Float64()*45, 1)   // 15-60 C
			generatorTemp[j] = roundTo(40+rand.Float64()*60, 1) // 40-100 C
		}

		payload := map[string]interface{}{
			"m": "wind_turbine",
			"columns": map[string]interface{}{
				"time":           times,
				"turbine_id":     turbineIDVals,
				"farm":           farmVals,
				"wind_speed":     windSpeed,
				"wind_direction": windDirection,
				"rotor_rpm":      rotorRPM,
				"power_output":   powerOutput,
				"blade_pitch":    bladePitch,
				"nacelle_temp":   nacelleTemp,
				"generator_temp": generatorTemp,
			},
		}

		data, err := msgpack.Marshal(payload)
		if err != nil {
			panic(err)
		}

		switch compress {
		case "gzip":
			var buf bytes.Buffer
			w, _ := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
			w.Write(data)
			w.Close()
			data = buf.Bytes()
		case "zstd":
			data = zstdEncoder.EncodeAll(data, nil)
		}

		batches[i] = data

		if (i+1)%100 == 0 {
			fmt.Printf("  Progress: %d/%d\n", i+1, count)
		}
	}

	return batches
}

func generateAerospaceBatches(count, batchSize int, compress string, zstdLevel int) [][]byte {
	// Aerospace/rocket telemetry - ultra-narrow schema for max throughput
	// Thousands of sensors, each reporting a single value
	sensorIDs := make([]string, 2000)
	for i := range sensorIDs {
		sensorIDs[i] = fmt.Sprintf("sens_%04d", i)
	}

	batches := make([][]byte, count)
	var zstdEncoder *zstd.Encoder
	if compress == "zstd" {
		var err error
		zstdEncoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(zstdLevel)))
		if err != nil {
			panic(err)
		}
	}

	for i := 0; i < count; i++ {
		nowMicros := time.Now().UnixMicro()

		times := make([]int64, batchSize)
		sensorIDVals := make([]string, batchSize)
		values := make([]float64, batchSize)

		for j := 0; j < batchSize; j++ {
			times[j] = nowMicros + int64(j)
			sensorIDVals[j] = sensorIDs[rand.Intn(len(sensorIDs))]
			values[j] = roundTo(rand.Float64()*1000, 2) // Generic sensor reading
		}

		payload := map[string]interface{}{
			"m": "rocket_telemetry",
			"columns": map[string]interface{}{
				"time":      times,
				"sensor_id": sensorIDVals,
				"value":     values,
			},
		}

		data, err := msgpack.Marshal(payload)
		if err != nil {
			panic(err)
		}

		switch compress {
		case "gzip":
			var buf bytes.Buffer
			w, _ := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
			w.Write(data)
			w.Close()
			data = buf.Bytes()
		case "zstd":
			data = zstdEncoder.EncodeAll(data, nil)
		}

		batches[i] = data

		if (i+1)%100 == 0 {
			fmt.Printf("  Progress: %d/%d\n", i+1, count)
		}
	}

	return batches
}

func generateIndustrialBatches(count, batchSize int, compress string, zstdLevel int) [][]byte {
	// Industrial pump telemetry - realistic fields for mining, oil & gas, manufacturing
	pumpIDs := make([]string, 100)
	for i := range pumpIDs {
		pumpIDs[i] = fmt.Sprintf("pump_%03d", i)
	}
	facilities := []string{"mine_alpha", "mine_beta", "refinery_1", "plant_east", "plant_west"}
	pumpTypes := []string{"centrifugal", "positive_displacement", "submersible", "diaphragm"}

	batches := make([][]byte, count)
	var zstdEncoder *zstd.Encoder
	if compress == "zstd" {
		var err error
		zstdEncoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(zstdLevel)))
		if err != nil {
			panic(err)
		}
	}

	for i := 0; i < count; i++ {
		nowMicros := time.Now().UnixMicro()

		times := make([]int64, batchSize)
		pumpIDVals := make([]string, batchSize)
		facilityVals := make([]string, batchSize)
		pumpTypeVals := make([]string, batchSize)
		flowRate := make([]float64, batchSize)    // GPM
		pressureIn := make([]float64, batchSize)  // PSI
		pressureOut := make([]float64, batchSize) // PSI
		temperature := make([]float64, batchSize) // Celsius
		vibration := make([]float64, batchSize)   // mm/s
		current := make([]float64, batchSize)     // Amps
		rpm := make([]int, batchSize)             // RPM
		power := make([]float64, batchSize)       // kW

		for j := 0; j < batchSize; j++ {
			times[j] = nowMicros + int64(j)
			pumpIDVals[j] = pumpIDs[rand.Intn(len(pumpIDs))]
			facilityVals[j] = facilities[rand.Intn(len(facilities))]
			pumpTypeVals[j] = pumpTypes[rand.Intn(len(pumpTypes))]
			flowRate[j] = roundTo(50+rand.Float64()*450, 1)     // 50-500 GPM
			pressureIn[j] = roundTo(10+rand.Float64()*40, 1)    // 10-50 PSI inlet
			pressureOut[j] = roundTo(100+rand.Float64()*400, 1) // 100-500 PSI outlet
			temperature[j] = roundTo(20+rand.Float64()*60, 1)   // 20-80 C
			vibration[j] = roundTo(rand.Float64()*10, 2)        // 0-10 mm/s
			current[j] = roundTo(10+rand.Float64()*90, 1)       // 10-100 Amps
			rpm[j] = 1000 + rand.Intn(2500)                     // 1000-3500 RPM
			power[j] = roundTo(5+rand.Float64()*95, 1)          // 5-100 kW
		}

		payload := map[string]interface{}{
			"m": "pump_telemetry",
			"columns": map[string]interface{}{
				"time":         times,
				"pump_id":      pumpIDVals,
				"facility":     facilityVals,
				"pump_type":    pumpTypeVals,
				"flow_rate":    flowRate,
				"pressure_in":  pressureIn,
				"pressure_out": pressureOut,
				"temperature":  temperature,
				"vibration":    vibration,
				"current":      current,
				"rpm":          rpm,
				"power":        power,
			},
		}

		data, err := msgpack.Marshal(payload)
		if err != nil {
			panic(err)
		}

		switch compress {
		case "gzip":
			var buf bytes.Buffer
			w, _ := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
			w.Write(data)
			w.Close()
			data = buf.Bytes()
		case "zstd":
			data = zstdEncoder.EncodeAll(data, nil)
		}

		batches[i] = data

		if (i+1)%100 == 0 {
			fmt.Printf("  Progress: %d/%d\n", i+1, count)
		}
	}

	return batches
}

func generateFinancialBatches(count, batchSize int, compress string, zstdLevel int) [][]byte {
	symbols := []string{
		"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "TSLA", "JPM", "V", "JNJ",
		"WMT", "PG", "UNH", "HD", "MA", "DIS", "PYPL", "BAC", "ADBE", "NFLX",
	}
	exchanges := []string{"NYSE", "NASDAQ", "ARCA", "BATS", "IEX"}

	batches := make([][]byte, count)
	var zstdEncoder *zstd.Encoder
	if compress == "zstd" {
		var err error
		zstdEncoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(zstdLevel)))
		if err != nil {
			panic(err)
		}
	}

	for i := 0; i < count; i++ {
		nowMicros := time.Now().UnixMicro()

		times := make([]int64, batchSize)
		symbolVals := make([]string, batchSize)
		exchangeVals := make([]string, batchSize)
		prices := make([]float64, batchSize)
		bids := make([]float64, batchSize)
		asks := make([]float64, batchSize)
		bidSizes := make([]int, batchSize)
		askSizes := make([]int, batchSize)
		volumes := make([]int, batchSize)
		tradeIDs := make([]int, batchSize)

		for j := 0; j < batchSize; j++ {
			times[j] = nowMicros + int64(j)
			symbolVals[j] = symbols[rand.Intn(len(symbols))]
			exchangeVals[j] = exchanges[rand.Intn(len(exchanges))]
			basePrice := roundTo(10+rand.Float64()*490, 2)
			prices[j] = basePrice
			bids[j] = roundTo(basePrice-rand.Float64()*0.04-0.01, 2)
			asks[j] = roundTo(basePrice+rand.Float64()*0.04+0.01, 2)
			bidSizes[j] = 100 + rand.Intn(9900)
			askSizes[j] = 100 + rand.Intn(9900)
			volumes[j] = 1 + rand.Intn(999)
			tradeIDs[j] = 1000000 + rand.Intn(8999999)
		}

		payload := map[string]interface{}{
			"m": "trades",
			"columns": map[string]interface{}{
				"time":     times,
				"symbol":   symbolVals,
				"exchange": exchangeVals,
				"price":    prices,
				"bid":      bids,
				"ask":      asks,
				"bid_size": bidSizes,
				"ask_size": askSizes,
				"volume":   volumes,
				"trade_id": tradeIDs,
			},
		}

		data, err := msgpack.Marshal(payload)
		if err != nil {
			panic(err)
		}

		switch compress {
		case "gzip":
			var buf bytes.Buffer
			w, _ := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
			w.Write(data)
			w.Close()
			data = buf.Bytes()
		case "zstd":
			data = zstdEncoder.EncodeAll(data, nil)
		}

		batches[i] = data

		if (i+1)%100 == 0 {
			fmt.Printf("  Progress: %d/%d\n", i+1, count)
		}
	}

	return batches
}

// ClickHouse table schemas per data type
var clickHouseSchemas = map[string]struct {
	table  string
	create string
}{
	"iot": {
		table: "cpu",
		create: `CREATE TABLE IF NOT EXISTS cpu (
			time DateTime64(6),
			host String,
			value Float64,
			cpu_idle Float64,
			cpu_user Float64
		) ENGINE = MergeTree() ORDER BY (host, time)`,
	},
	"financial": {
		table: "trades",
		create: `CREATE TABLE IF NOT EXISTS trades (
			time DateTime64(6),
			symbol String,
			exchange String,
			price Float64,
			bid Float64,
			ask Float64,
			bid_size Int64,
			ask_size Int64,
			volume Int64,
			trade_id Int64
		) ENGINE = MergeTree() ORDER BY (symbol, time)`,
	},
	"industrial": {
		table: "pump_telemetry",
		create: `CREATE TABLE IF NOT EXISTS pump_telemetry (
			time DateTime64(6),
			pump_id String,
			facility String,
			pump_type String,
			flow_rate Float64,
			pressure_in Float64,
			pressure_out Float64,
			temperature Float64,
			vibration Float64,
			current Float64,
			rpm Int64,
			power Float64
		) ENGINE = MergeTree() ORDER BY (pump_id, time)`,
	},
	"aerospace": {
		table: "rocket_telemetry",
		create: `CREATE TABLE IF NOT EXISTS rocket_telemetry (
			time DateTime64(6),
			sensor_id String,
			value Float64
		) ENGINE = MergeTree() ORDER BY (sensor_id, time)`,
	},
	"energy": {
		table: "wind_turbine",
		create: `CREATE TABLE IF NOT EXISTS wind_turbine (
			time DateTime64(6),
			turbine_id String,
			farm String,
			wind_speed Float64,
			wind_direction Float64,
			rotor_rpm Float64,
			power_output Float64,
			blade_pitch Float64,
			nacelle_temp Float64,
			generator_temp Float64
		) ENGINE = MergeTree() ORDER BY (turbine_id, time)`,
	},
	"racing": {
		table: "car_telemetry",
		create: `CREATE TABLE IF NOT EXISTS car_telemetry (
			time DateTime64(6),
			car_number String,
			driver String,
			speed Float64,
			engine_rpm Float64,
			throttle Float64,
			brake Float64,
			steering Float64,
			gear Int64
		) ENGINE = MergeTree() ORDER BY (car_number, time)`,
	},
}

func ensureClickHouseTable(conn clickhouse.Conn, dataType string) (string, error) {
	schema, ok := clickHouseSchemas[dataType]
	if !ok {
		return "", fmt.Errorf("unsupported data type for ClickHouse: %s", dataType)
	}
	if err := conn.Exec(context.Background(), schema.create); err != nil {
		return "", fmt.Errorf("create table: %w", err)
	}
	return schema.table, nil
}

// Columnar batch for ClickHouse native protocol.
// Each column is a typed slice matching the CREATE TABLE column order.
type columnBatch struct {
	columns []interface{} // Each element is a typed slice ([]time.Time, []string, []float64, []int64)
}

func generateIOTColumnBatches(count, batchSize int) []columnBatch {
	hosts := make([]string, 1000)
	for i := range hosts {
		hosts[i] = fmt.Sprintf("server%03d", i)
	}

	batches := make([]columnBatch, count)
	for i := 0; i < count; i++ {
		now := time.Now()
		times := make([]time.Time, batchSize)
		hostVals := make([]string, batchSize)
		values := make([]float64, batchSize)
		cpuIdle := make([]float64, batchSize)
		cpuUser := make([]float64, batchSize)

		for j := 0; j < batchSize; j++ {
			times[j] = now.Add(time.Duration(j) * time.Microsecond)
			hostVals[j] = hosts[rand.Intn(len(hosts))]
			values[j] = roundTo(rand.Float64()*100, 2)
			cpuIdle[j] = roundTo(rand.Float64()*100, 2)
			cpuUser[j] = roundTo(rand.Float64()*100, 2)
		}

		batches[i] = columnBatch{columns: []interface{}{times, hostVals, values, cpuIdle, cpuUser}}
		if (i+1)%100 == 0 {
			fmt.Printf("  Progress: %d/%d\n", i+1, count)
		}
	}
	return batches
}

func generateFinancialColumnBatches(count, batchSize int) []columnBatch {
	symbols := []string{
		"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "TSLA", "JPM", "V", "JNJ",
		"WMT", "PG", "UNH", "HD", "MA", "DIS", "PYPL", "BAC", "ADBE", "NFLX",
	}
	exchanges := []string{"NYSE", "NASDAQ", "ARCA", "BATS", "IEX"}

	batches := make([]columnBatch, count)
	for i := 0; i < count; i++ {
		now := time.Now()
		times := make([]time.Time, batchSize)
		symbolVals := make([]string, batchSize)
		exchangeVals := make([]string, batchSize)
		prices := make([]float64, batchSize)
		bids := make([]float64, batchSize)
		asks := make([]float64, batchSize)
		bidSizes := make([]int64, batchSize)
		askSizes := make([]int64, batchSize)
		volumes := make([]int64, batchSize)
		tradeIDs := make([]int64, batchSize)

		for j := 0; j < batchSize; j++ {
			times[j] = now.Add(time.Duration(j) * time.Microsecond)
			symbolVals[j] = symbols[rand.Intn(len(symbols))]
			exchangeVals[j] = exchanges[rand.Intn(len(exchanges))]
			basePrice := roundTo(10+rand.Float64()*490, 2)
			prices[j] = basePrice
			bids[j] = roundTo(basePrice-rand.Float64()*0.04-0.01, 2)
			asks[j] = roundTo(basePrice+rand.Float64()*0.04+0.01, 2)
			bidSizes[j] = int64(100 + rand.Intn(9900))
			askSizes[j] = int64(100 + rand.Intn(9900))
			volumes[j] = int64(1 + rand.Intn(999))
			tradeIDs[j] = int64(1000000 + rand.Intn(8999999))
		}

		batches[i] = columnBatch{columns: []interface{}{times, symbolVals, exchangeVals, prices, bids, asks, bidSizes, askSizes, volumes, tradeIDs}}
		if (i+1)%100 == 0 {
			fmt.Printf("  Progress: %d/%d\n", i+1, count)
		}
	}
	return batches
}

func generateIndustrialColumnBatches(count, batchSize int) []columnBatch {
	pumpIDs := make([]string, 100)
	for i := range pumpIDs {
		pumpIDs[i] = fmt.Sprintf("pump_%03d", i)
	}
	facilities := []string{"mine_alpha", "mine_beta", "refinery_1", "plant_east", "plant_west"}
	pumpTypes := []string{"centrifugal", "positive_displacement", "submersible", "diaphragm"}

	batches := make([]columnBatch, count)
	for i := 0; i < count; i++ {
		now := time.Now()
		times := make([]time.Time, batchSize)
		pumpIDVals := make([]string, batchSize)
		facilityVals := make([]string, batchSize)
		pumpTypeVals := make([]string, batchSize)
		flowRate := make([]float64, batchSize)
		pressureIn := make([]float64, batchSize)
		pressureOut := make([]float64, batchSize)
		temperature := make([]float64, batchSize)
		vibration := make([]float64, batchSize)
		current := make([]float64, batchSize)
		rpm := make([]int64, batchSize)
		power := make([]float64, batchSize)

		for j := 0; j < batchSize; j++ {
			times[j] = now.Add(time.Duration(j) * time.Microsecond)
			pumpIDVals[j] = pumpIDs[rand.Intn(len(pumpIDs))]
			facilityVals[j] = facilities[rand.Intn(len(facilities))]
			pumpTypeVals[j] = pumpTypes[rand.Intn(len(pumpTypes))]
			flowRate[j] = roundTo(50+rand.Float64()*450, 1)
			pressureIn[j] = roundTo(10+rand.Float64()*40, 1)
			pressureOut[j] = roundTo(100+rand.Float64()*400, 1)
			temperature[j] = roundTo(20+rand.Float64()*60, 1)
			vibration[j] = roundTo(rand.Float64()*10, 2)
			current[j] = roundTo(10+rand.Float64()*90, 1)
			rpm[j] = int64(1000 + rand.Intn(2500))
			power[j] = roundTo(5+rand.Float64()*95, 1)
		}

		batches[i] = columnBatch{columns: []interface{}{times, pumpIDVals, facilityVals, pumpTypeVals, flowRate, pressureIn, pressureOut, temperature, vibration, current, rpm, power}}
		if (i+1)%100 == 0 {
			fmt.Printf("  Progress: %d/%d\n", i+1, count)
		}
	}
	return batches
}

func generateAerospaceColumnBatches(count, batchSize int) []columnBatch {
	sensorIDs := make([]string, 2000)
	for i := range sensorIDs {
		sensorIDs[i] = fmt.Sprintf("sens_%04d", i)
	}

	batches := make([]columnBatch, count)
	for i := 0; i < count; i++ {
		now := time.Now()
		times := make([]time.Time, batchSize)
		sensorIDVals := make([]string, batchSize)
		values := make([]float64, batchSize)

		for j := 0; j < batchSize; j++ {
			times[j] = now.Add(time.Duration(j) * time.Microsecond)
			sensorIDVals[j] = sensorIDs[rand.Intn(len(sensorIDs))]
			values[j] = roundTo(rand.Float64()*1000, 2)
		}

		batches[i] = columnBatch{columns: []interface{}{times, sensorIDVals, values}}
		if (i+1)%100 == 0 {
			fmt.Printf("  Progress: %d/%d\n", i+1, count)
		}
	}
	return batches
}

func generateEnergyColumnBatches(count, batchSize int) []columnBatch {
	turbineIDs := make([]string, 200)
	for i := range turbineIDs {
		turbineIDs[i] = fmt.Sprintf("turbine_%03d", i)
	}
	farms := []string{"north_ridge", "coastal_1", "plains_west", "offshore_a", "hilltop"}

	batches := make([]columnBatch, count)
	for i := 0; i < count; i++ {
		now := time.Now()
		times := make([]time.Time, batchSize)
		turbineIDVals := make([]string, batchSize)
		farmVals := make([]string, batchSize)
		windSpeed := make([]float64, batchSize)
		windDirection := make([]float64, batchSize)
		rotorRPM := make([]float64, batchSize)
		powerOutput := make([]float64, batchSize)
		bladePitch := make([]float64, batchSize)
		nacelleTemp := make([]float64, batchSize)
		generatorTemp := make([]float64, batchSize)

		for j := 0; j < batchSize; j++ {
			times[j] = now.Add(time.Duration(j) * time.Microsecond)
			turbineIDVals[j] = turbineIDs[rand.Intn(len(turbineIDs))]
			farmVals[j] = farms[rand.Intn(len(farms))]
			windSpeed[j] = roundTo(2+rand.Float64()*23, 1)
			windDirection[j] = roundTo(rand.Float64()*360, 1)
			rotorRPM[j] = roundTo(5+rand.Float64()*15, 1)
			powerOutput[j] = roundTo(rand.Float64()*5000, 1)
			bladePitch[j] = roundTo(rand.Float64()*25, 1)
			nacelleTemp[j] = roundTo(15+rand.Float64()*45, 1)
			generatorTemp[j] = roundTo(40+rand.Float64()*60, 1)
		}

		batches[i] = columnBatch{columns: []interface{}{times, turbineIDVals, farmVals, windSpeed, windDirection, rotorRPM, powerOutput, bladePitch, nacelleTemp, generatorTemp}}
		if (i+1)%100 == 0 {
			fmt.Printf("  Progress: %d/%d\n", i+1, count)
		}
	}
	return batches
}

func generateRacingColumnBatches(count, batchSize int) []columnBatch {
	carNumbers := []string{"1", "4", "11", "14", "16", "22", "44", "55", "63", "77",
		"10", "18", "20", "23", "24", "27", "31", "40", "81", "2"}
	drivers := []string{"VER", "NOR", "PER", "ALO", "LEC", "TSU", "HAM", "SAI", "RUS", "BOT",
		"GAS", "STR", "MAG", "HUL", "RIC", "ALB", "SAR", "ZHO", "PIA", "LAW"}

	batches := make([]columnBatch, count)
	for i := 0; i < count; i++ {
		now := time.Now()
		times := make([]time.Time, batchSize)
		carNumberVals := make([]string, batchSize)
		driverVals := make([]string, batchSize)
		speed := make([]float64, batchSize)
		engineRPM := make([]float64, batchSize)
		throttle := make([]float64, batchSize)
		brake := make([]float64, batchSize)
		steering := make([]float64, batchSize)
		gear := make([]int64, batchSize)

		for j := 0; j < batchSize; j++ {
			idx := rand.Intn(len(carNumbers))
			times[j] = now.Add(time.Duration(j) * time.Microsecond)
			carNumberVals[j] = carNumbers[idx]
			driverVals[j] = drivers[idx]
			speed[j] = roundTo(50+rand.Float64()*300, 1)
			engineRPM[j] = roundTo(float64(8000+rand.Intn(7000)), 0)
			throttle[j] = roundTo(rand.Float64()*100, 1)
			brake[j] = roundTo(rand.Float64()*100, 1)
			steering[j] = roundTo(-180+rand.Float64()*360, 1)
			gear[j] = int64(1 + rand.Intn(8))
		}

		batches[i] = columnBatch{columns: []interface{}{times, carNumberVals, driverVals, speed, engineRPM, throttle, brake, steering, gear}}
		if (i+1)%100 == 0 {
			fmt.Printf("  Progress: %d/%d\n", i+1, count)
		}
	}
	return batches
}

func generateColumnBatches(dataType string, count, batchSize int) []columnBatch {
	switch dataType {
	case "financial":
		return generateFinancialColumnBatches(count, batchSize)
	case "industrial":
		return generateIndustrialColumnBatches(count, batchSize)
	case "aerospace":
		return generateAerospaceColumnBatches(count, batchSize)
	case "energy":
		return generateEnergyColumnBatches(count, batchSize)
	case "racing":
		return generateRacingColumnBatches(count, batchSize)
	default:
		return generateIOTColumnBatches(count, batchSize)
	}
}

func clickhouseWorker(id int, cfg *Config, colBatches []columnBatch, stats *Stats, tableName string) {
	settings := clickhouse.Settings{
		"max_execution_time": 60,
	}
	if cfg.AsyncInsert {
		settings["async_insert"] = 1
		settings["wait_for_async_insert"] = 1
		settings["async_insert_busy_timeout_ms"] = 200
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.CHPort)},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
		},
		Settings: settings,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		DialTimeout:     10 * time.Second,
		MaxOpenConns:    1,
		MaxIdleConns:    1,
		ConnMaxLifetime: time.Hour,
	})
	if err != nil {
		fmt.Printf("Worker %d: failed to connect: %v\n", id, err)
		return
	}
	defer conn.Close()

	ctx := context.Background()
	batchIdx := 0

	for stats.running.Load() {
		cb := colBatches[batchIdx%len(colBatches)]
		batchIdx++

		start := time.Now()

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+tableName)
		if err != nil {
			stats.totalErrors.Add(1)
			if stats.totalErrors.Load() <= 3 {
				fmt.Printf("Worker %d: PrepareBatch error: %v\n", id, err)
			}
			continue
		}

		// Columnar append — pass entire typed slices instead of row-by-row
		appendErr := false
		for colIdx, colData := range cb.columns {
			if err := batch.Column(colIdx).Append(colData); err != nil {
				stats.totalErrors.Add(1)
				if stats.totalErrors.Load() <= 3 {
					fmt.Printf("Worker %d: Column(%d) Append error: %v\n", id, colIdx, err)
				}
				appendErr = true
				break
			}
		}
		if appendErr {
			continue
		}

		if err := batch.Send(); err != nil {
			stats.totalErrors.Add(1)
			if stats.totalErrors.Load() <= 3 {
				fmt.Printf("Worker %d: Send error: %v\n", id, err)
			}
			continue
		}

		latencyMs := float64(time.Since(start).Microseconds()) / 1000.0
		stats.totalSent.Add(int64(cfg.BatchSize))
		stats.addLatency(id, latencyMs)
	}
}

// generateClickHouseJSONBatches produces JSONEachRow payloads for ClickHouse HTTP interface.
func generateClickHouseJSONBatches(dataType string, count, batchSize int) [][]byte {
	batches := make([][]byte, count)

	for i := 0; i < count; i++ {
		var buf bytes.Buffer
		now := time.Now()

		for j := 0; j < batchSize; j++ {
			ts := now.Add(time.Duration(j) * time.Microsecond).UTC().Format("2006-01-02 15:04:05.000000")
			var row map[string]interface{}

			switch dataType {
			case "financial":
				symbols := []string{"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "TSLA", "JPM", "V", "JNJ",
					"WMT", "PG", "UNH", "HD", "MA", "DIS", "PYPL", "BAC", "ADBE", "NFLX"}
				exchanges := []string{"NYSE", "NASDAQ", "ARCA", "BATS", "IEX"}
				basePrice := roundTo(10+rand.Float64()*490, 2)
				row = map[string]interface{}{
					"time": ts, "symbol": symbols[rand.Intn(len(symbols))], "exchange": exchanges[rand.Intn(len(exchanges))],
					"price": basePrice, "bid": roundTo(basePrice-rand.Float64()*0.04-0.01, 2),
					"ask": roundTo(basePrice+rand.Float64()*0.04+0.01, 2),
					"bid_size": 100 + rand.Intn(9900), "ask_size": 100 + rand.Intn(9900),
					"volume": 1 + rand.Intn(999), "trade_id": 1000000 + rand.Intn(8999999),
				}
			case "aerospace":
				row = map[string]interface{}{
					"time": ts, "sensor_id": fmt.Sprintf("sens_%04d", rand.Intn(2000)),
					"value": roundTo(rand.Float64()*1000, 2),
				}
			default: // iot
				row = map[string]interface{}{
					"time": ts, "host": fmt.Sprintf("server%03d", rand.Intn(1000)),
					"value": roundTo(rand.Float64()*100, 2), "cpu_idle": roundTo(rand.Float64()*100, 2),
					"cpu_user": roundTo(rand.Float64()*100, 2),
				}
			}

			docBytes, _ := json.Marshal(row)
			buf.Write(docBytes)
			buf.WriteByte('\n')
		}

		batches[i] = buf.Bytes()
		if (i+1)%100 == 0 {
			fmt.Printf("  Progress: %d/%d\n", i+1, count)
		}
	}
	return batches
}

func clickhouseHTTPWorker(id int, cfg *Config, batches [][]byte, stats *Stats, client *http.Client, tableName string) {
	url := fmt.Sprintf("http://%s:%d/?query=INSERT+INTO+%s+FORMAT+JSONEachRow", cfg.Host, cfg.CHPort, tableName)
	batchIdx := 0

	for stats.running.Load() {
		batch := batches[batchIdx%len(batches)]
		batchIdx++

		start := time.Now()

		req, err := http.NewRequest("POST", url, bytes.NewReader(batch))
		if err != nil {
			stats.totalErrors.Add(1)
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			stats.totalErrors.Add(1)
			if stats.totalErrors.Load() <= 3 {
				fmt.Printf("Worker %d: HTTP error: %v\n", id, err)
			}
			continue
		}

		latencyMs := float64(time.Since(start).Microseconds()) / 1000.0

		if resp.StatusCode == 200 {
			stats.totalSent.Add(int64(cfg.BatchSize))
			stats.addLatency(id, latencyMs)
		} else {
			stats.totalErrors.Add(1)
			if stats.totalErrors.Load() <= 3 {
				body, _ := io.ReadAll(resp.Body)
				fmt.Printf("Worker %d: HTTP %d: %s\n", id, resp.StatusCode, string(body)[:min(200, len(body))])
			}
		}
		resp.Body.Close()
	}
}

func worker(id int, cfg *Config, batches [][]byte, stats *Stats, client *http.Client) {
	var url string
	var contentType string

	scheme := "http"
	if cfg.TLS {
		scheme = "https"
	}

	if cfg.Protocol == "lineprotocol" {
		url = fmt.Sprintf("%s://%s:%d/api/v1/write", scheme, cfg.Host, cfg.Port)
		contentType = "text/plain"
	} else {
		url = fmt.Sprintf("%s://%s:%d/api/v1/write/msgpack", scheme, cfg.Host, cfg.Port)
		contentType = "application/msgpack"
	}

	batchIdx := 0

	headers := map[string]string{
		"Content-Type":   contentType,
		"x-arc-database": "production",
	}
	if cfg.Token != "" {
		headers["Authorization"] = "Bearer " + cfg.Token
	}
	if cfg.Compress == "gzip" {
		headers["Content-Encoding"] = "gzip"
	} else if cfg.Compress == "zstd" {
		headers["Content-Encoding"] = "zstd"
	}

	for stats.running.Load() {
		batch := batches[batchIdx%len(batches)]
		batchIdx++

		start := time.Now()

		req, err := http.NewRequest("POST", url, bytes.NewReader(batch))
		if err != nil {
			stats.totalErrors.Add(1)
			continue
		}

		for k, v := range headers {
			req.Header.Set(k, v)
		}

		resp, err := client.Do(req)
		if err != nil {
			stats.totalErrors.Add(1)
			if stats.totalErrors.Load() <= 3 {
				fmt.Printf("Error: %v\n", err)
			}
			continue
		}

		latencyMs := float64(time.Since(start).Microseconds()) / 1000.0

		if resp.StatusCode == 204 {
			stats.totalSent.Add(int64(cfg.BatchSize))
			stats.addLatency(id, latencyMs)
		} else {
			stats.totalErrors.Add(1)
			if stats.totalErrors.Load() <= 3 {
				body, _ := io.ReadAll(resp.Body)
				fmt.Printf("Error %d: %s\n", resp.StatusCode, string(body)[:min(100, len(body))])
			}
		}
		resp.Body.Close()
	}
}

func main() {
	cfg := Config{}

	flag.IntVar(&cfg.Duration, "duration", 60, "Test duration in seconds")
	flag.IntVar(&cfg.Workers, "workers", 100, "Number of concurrent workers")
	flag.IntVar(&cfg.BatchSize, "batch-size", 1000, "Records per batch")
	flag.IntVar(&cfg.Pregenerate, "pregenerate", 1000, "Number of batches to pre-generate")
	flag.StringVar(&cfg.Compress, "compress", "none", "Compression: none, gzip, zstd")
	flag.IntVar(&cfg.ZstdLevel, "zstd-level", 3, "Zstd compression level (1-22)")
	flag.StringVar(&cfg.DataType, "data-type", "iot", "Data type: iot, financial, industrial, aerospace, energy, racing")
	flag.StringVar(&cfg.Protocol, "protocol", "msgpack", "Protocol: msgpack, lineprotocol")
	flag.StringVar(&cfg.Target, "target", "arc", "Target: arc, clickhouse, clickhouse-http")
	flag.StringVar(&cfg.Host, "host", "localhost", "Server host")
	flag.IntVar(&cfg.Port, "port", 8000, "Server port (Arc HTTP)")
	flag.IntVar(&cfg.CHPort, "ch-port", 9000, "ClickHouse native port")
	flag.StringVar(&cfg.Database, "database", "default", "ClickHouse database name")
	flag.BoolVar(&cfg.AsyncInsert, "async-insert", false, "Enable ClickHouse async_insert (server-side batching)")
	flag.BoolVar(&cfg.TLS, "tls", false, "Use HTTPS instead of HTTP")
	flag.Parse()

	cfg.Token = os.Getenv("ARC_TOKEN")

	// Compression label
	compressLabel := "No Compression"
	if cfg.Compress == "gzip" {
		compressLabel = "WITH GZIP COMPRESSION"
	} else if cfg.Compress == "zstd" {
		compressLabel = fmt.Sprintf("WITH ZSTD COMPRESSION (level=%d)", cfg.ZstdLevel)
	}

	dataTypeLabel := "IOT (Server Metrics)"
	if cfg.DataType == "financial" {
		dataTypeLabel = "FINANCIAL (Stock Prices)"
	} else if cfg.DataType == "industrial" {
		dataTypeLabel = "INDUSTRIAL (Pump Telemetry)"
	} else if cfg.DataType == "aerospace" {
		dataTypeLabel = "AEROSPACE (Rocket Telemetry)"
	} else if cfg.DataType == "energy" {
		dataTypeLabel = "ENERGY (Wind Turbine)"
	} else if cfg.DataType == "racing" {
		dataTypeLabel = "RACING (F1/Motorsport)"
	}

	protocolLabel := "MessagePack Columnar"
	endpoint := "/api/v1/write/msgpack"
	if cfg.Protocol == "lineprotocol" {
		protocolLabel = "Line Protocol"
		endpoint = "/api/v1/write"
	}

	fmt.Println("================================================================================")
	if cfg.Target == "clickhouse" {
		asyncLabel := ""
		if cfg.AsyncInsert {
			asyncLabel = " + ASYNC INSERT"
		}
		fmt.Printf("SUSTAINED LOAD TEST - CLICKHOUSE NATIVE PROTOCOL (LZ4%s)\n", asyncLabel)
		fmt.Println("================================================================================")
		fmt.Printf("Target: clickhouse://%s:%d (database: %s)\n", cfg.Host, cfg.CHPort, cfg.Database)
	} else if cfg.Target == "clickhouse-http" {
		fmt.Println("SUSTAINED LOAD TEST - CLICKHOUSE HTTP (JSONEachRow)")
		fmt.Println("================================================================================")
		fmt.Printf("Target: http://%s:%d (database: %s)\n", cfg.Host, cfg.CHPort, cfg.Database)
	} else {
		fmt.Printf("SUSTAINED LOAD TEST - GO CLIENT (%s)\n", compressLabel)
		fmt.Println("================================================================================")
		scheme := "http"
		if cfg.TLS {
			scheme = "https"
		}
		fmt.Printf("Target: %s://%s:%d%s\n", scheme, cfg.Host, cfg.Port, endpoint)
		fmt.Printf("Protocol: %s\n", protocolLabel)
	}
	fmt.Printf("Data type: %s\n", dataTypeLabel)
	fmt.Printf("Duration: %ds\n", cfg.Duration)
	fmt.Printf("Batch size: %d\n", cfg.BatchSize)
	fmt.Printf("Workers: %d\n", cfg.Workers)
	fmt.Printf("Pre-generate: %d batches\n", cfg.Pregenerate)
	fmt.Println("================================================================================")
	fmt.Println()

	// Generate batches
	fmt.Printf("Pre-generating %d batches...\n", cfg.Pregenerate)
	startGen := time.Now()

	stats := &Stats{}
	stats.initWorkers(cfg.Workers)
	stats.running.Store(true)

	var wg sync.WaitGroup

	if cfg.Target == "clickhouse" {
		// ClickHouse native protocol path — columnar inserts
		colBatches := generateColumnBatches(cfg.DataType, cfg.Pregenerate, cfg.BatchSize)

		genTime := time.Since(startGen)
		fmt.Printf("✓ Generated %d batches in %.1fs\n", cfg.Pregenerate, genTime.Seconds())
		fmt.Printf("  %d rows per batch (columnar native protocol)\n", cfg.BatchSize)
		fmt.Println()

		// Create table
		setupConn, err := clickhouse.Open(&clickhouse.Options{
			Addr: []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.CHPort)},
			Auth: clickhouse.Auth{Database: cfg.Database},
		})
		if err != nil {
			fmt.Printf("Failed to connect to ClickHouse: %v\n", err)
			os.Exit(1)
		}
		tableName, err := ensureClickHouseTable(setupConn, cfg.DataType)
		if err != nil {
			fmt.Printf("Failed to create table: %v\n", err)
			os.Exit(1)
		}
		setupConn.Close()
		fmt.Printf("✓ Table '%s' ready in database '%s'\n", tableName, cfg.Database)
		fmt.Println()

		// Launch ClickHouse workers
		fmt.Println("Starting test...")
		for i := 0; i < cfg.Workers; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				clickhouseWorker(id, &cfg, colBatches, stats, tableName)
			}(i)
		}
	} else if cfg.Target == "clickhouse-http" {
		// ClickHouse HTTP interface — JSONEachRow
		jsonBatches := generateClickHouseJSONBatches(cfg.DataType, cfg.Pregenerate, cfg.BatchSize)

		genTime := time.Since(startGen)
		var totalSize int64
		for _, b := range jsonBatches {
			totalSize += int64(len(b))
		}
		avgSize := float64(totalSize) / float64(len(jsonBatches))
		fmt.Printf("✓ Generated %d batches in %.1fs\n", cfg.Pregenerate, genTime.Seconds())
		fmt.Printf("  Avg size: %.1f KB (JSONEachRow)\n", avgSize/1024)
		fmt.Println()

		// Create table via native connection
		setupConn, err := clickhouse.Open(&clickhouse.Options{
			Addr: []string{fmt.Sprintf("%s:9000", cfg.Host)},
			Auth: clickhouse.Auth{Database: cfg.Database},
		})
		if err != nil {
			fmt.Printf("Failed to connect to ClickHouse: %v\n", err)
			os.Exit(1)
		}
		tableName, err := ensureClickHouseTable(setupConn, cfg.DataType)
		if err != nil {
			fmt.Printf("Failed to create table: %v\n", err)
			os.Exit(1)
		}
		setupConn.Close()
		fmt.Printf("✓ Table '%s' ready in database '%s'\n", tableName, cfg.Database)
		fmt.Println()

		// HTTP client for ClickHouse
		client := &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        cfg.Workers + 50,
				MaxIdleConnsPerHost: cfg.Workers + 50,
				MaxConnsPerHost:     cfg.Workers + 50,
				IdleConnTimeout:     30 * time.Second,
			},
			Timeout: 120 * time.Second,
		}

		fmt.Println("Starting test...")
		for i := 0; i < cfg.Workers; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				clickhouseHTTPWorker(id, &cfg, jsonBatches, stats, client, tableName)
			}(i)
		}
	} else {
		// Arc HTTP path
		var batches [][]byte
		if cfg.Protocol == "lineprotocol" {
			batches = generateLineProtocolBatches(cfg.Pregenerate, cfg.BatchSize, cfg.Compress, cfg.ZstdLevel, cfg.DataType)
		} else if cfg.DataType == "financial" {
			batches = generateFinancialBatches(cfg.Pregenerate, cfg.BatchSize, cfg.Compress, cfg.ZstdLevel)
		} else if cfg.DataType == "industrial" {
			batches = generateIndustrialBatches(cfg.Pregenerate, cfg.BatchSize, cfg.Compress, cfg.ZstdLevel)
		} else if cfg.DataType == "aerospace" {
			batches = generateAerospaceBatches(cfg.Pregenerate, cfg.BatchSize, cfg.Compress, cfg.ZstdLevel)
		} else if cfg.DataType == "energy" {
			batches = generateEnergyBatches(cfg.Pregenerate, cfg.BatchSize, cfg.Compress, cfg.ZstdLevel)
		} else if cfg.DataType == "racing" {
			batches = generateRacingBatches(cfg.Pregenerate, cfg.BatchSize, cfg.Compress, cfg.ZstdLevel)
		} else {
			batches = generateIOTBatches(cfg.Pregenerate, cfg.BatchSize, cfg.Compress, cfg.ZstdLevel)
		}

		genTime := time.Since(startGen)
		var totalSize int64
		for _, b := range batches {
			totalSize += int64(len(b))
		}
		avgSize := float64(totalSize) / float64(len(batches))

		sizeLabel := "uncompressed"
		if cfg.Compress != "none" {
			sizeLabel = cfg.Compress
		}
		fmt.Printf("✓ Generated %d batches in %.1fs\n", cfg.Pregenerate, genTime.Seconds())
		fmt.Printf("  Avg size: %.1f KB (%s)\n", avgSize/1024, sizeLabel)
		fmt.Println()

		if cfg.Token != "" {
			fmt.Printf("Using auth token: %s...\n", cfg.Token[:min(8, len(cfg.Token))])
		} else {
			fmt.Println("No ARC_TOKEN set - authentication may fail")
		}

		// Create HTTP client with connection pooling
		transport := &http.Transport{
			MaxIdleConns:        cfg.Workers + 50,
			MaxIdleConnsPerHost: cfg.Workers + 50,
			MaxConnsPerHost:     cfg.Workers + 50,
			IdleConnTimeout:     30 * time.Second,
			DisableKeepAlives:   false,
		}
		if cfg.TLS {
			// Benchmark tool only — connects to local/dev instances with self-signed certs
			transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec
		}
		// Resolve *.localhost to 127.0.0.1 (browsers do this per RFC 6761, Go doesn't)
		if strings.HasSuffix(cfg.Host, ".localhost") {
			dialer := &net.Dialer{Timeout: 10 * time.Second}
			transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
				_, port, _ := net.SplitHostPort(addr)
				return dialer.DialContext(ctx, network, net.JoinHostPort("127.0.0.1", port))
			}
		}
		client := &http.Client{
			Transport: transport,
			Timeout:   120 * time.Second,
		}

		// Launch Arc workers
		fmt.Println("Starting test...")
		for i := 0; i < cfg.Workers; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				worker(id, &cfg, batches, stats, client)
			}(i)
		}
	}

	// Progress reporting
	startTime := time.Now()
	lastSent := int64(0)
	ticker := time.NewTicker(5 * time.Second)

	go func() {
		for range ticker.C {
			if !stats.running.Load() {
				return
			}
			elapsed := time.Since(startTime).Seconds()
			currentSent := stats.totalSent.Load()
			intervalRPS := float64(currentSent-lastSent) / 5.0
			fmt.Printf("[%6.1fs] RPS: %10.0f | Total: %12d | Errors: %6d\n",
				elapsed, intervalRPS, currentSent, stats.totalErrors.Load())
			lastSent = currentSent
		}
	}()

	// Wait for duration
	time.Sleep(time.Duration(cfg.Duration) * time.Second)
	stats.running.Store(false)
	ticker.Stop()

	// Wait for workers to finish
	wg.Wait()

	elapsed := time.Since(startTime).Seconds()
	totalSent := stats.totalSent.Load()
	totalErrors := stats.totalErrors.Load()

	// Results
	fmt.Println()
	fmt.Println("================================================================================")
	fmt.Println("RESULTS")
	fmt.Println("================================================================================")
	fmt.Printf("Duration:        %.1fs\n", elapsed)
	fmt.Printf("Total sent:      %d records\n", totalSent)
	fmt.Printf("Total errors:    %d\n", totalErrors)
	successRate := float64(totalSent) / float64(totalSent+max(totalErrors, 1)) * 100
	fmt.Printf("Success rate:    %.2f%%\n", successRate)
	fmt.Println()
	fmt.Printf("🚀 THROUGHPUT:   %d records/sec\n", int64(float64(totalSent)/elapsed))
	fmt.Println()
	fmt.Println("Latency percentiles:")
	fmt.Printf("  p50:  %.2f ms\n", stats.getPercentile(0.50))
	fmt.Printf("  p95:  %.2f ms\n", stats.getPercentile(0.95))
	fmt.Printf("  p99:  %.2f ms\n", stats.getPercentile(0.99))
	fmt.Printf("  p999: %.2f ms\n", stats.getPercentile(0.999))
	fmt.Println("================================================================================")
}
