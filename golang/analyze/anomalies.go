package analyze

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"time"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Weather struct {
	Temperature *float64
	Pressure    *float64
	Humidity    *float64
	WindSpeed   *float64
}

type APIWeather struct {
	ID          int       `json:"id"`
	LocId       int       `json:"loc_id"`
	Lat         float64   `json:"lat"`
	Lon         float64   `json:"lon"`
	Temperature *float64  `json:"temperature"`
	Pressure    *float64  `json:"pressure"`
	Humidity    *float64  `json:"humidity"`
	WindSpeed   *float64  `json:"wind_speed"`
	CollectedAt time.Time `json:"collected_at"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}


func Sum[T int | float64](slice []T) T {
	var result T
	for _, value := range slice {
		result += value
	}
	return result
}

func ConnectToPostgres() *pgxpool.Pool {
	ctx := context.Background()
	dsn := os.Getenv("DATABASE_URL_FOR_GO")
	if dsn == "" {
		dsn = "postgresql://weather_user:weather_pass@weather_db:5432/weather_guard"
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	// defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to Postgres")
	return pool
}
func isAnomaly(val *float64, avg, threshold float64, useAbsAvg bool) bool {
	if val == nil {
		return false
	}
	diff := math.Abs(*val - avg)
	if useAbsAvg {
		return diff > threshold*math.Abs(avg)
	}
	return diff > threshold*avg
}

func compareWeather(pool *pgxpool.Pool, ctx context.Context, data APIWeather) map[string]any {
	lat := data.Lat
	lon := data.Lon
	temperature := data.Temperature
	pressure := data.Pressure
	humidity := data.Humidity
	windSpeed := data.WindSpeed

	radius := 0.3

	similarPlacesRows, err := pool.Query(ctx,
		`SELECT temperature, pressure, humidity, wind_speed FROM weather
		WHERE lat >= $1 AND lat <= $2 AND lon >= $3 AND lon <= $4 AND collected_at > $5`,
		lat-radius, lat+radius, lon-radius, lon+radius, time.Now().UTC().AddDate(0, 0, -31))
	if err != nil {
		log.Fatal(err)
	}
	defer similarPlacesRows.Close()

	var similarPlaces []Weather
	for similarPlacesRows.Next() {
		var w Weather
		if err := similarPlacesRows.Scan(&w.Temperature, &w.Pressure, &w.Humidity, &w.WindSpeed); err != nil {
			log.Fatal(err)
		}
		similarPlaces = append(similarPlaces, w)
	}
	if similarPlacesRows.Err() != nil {
		log.Fatal(similarPlacesRows.Err())
	}
	if len(similarPlaces) < 2 {
		fmt.Println("Похожие места не найдены или их не хватает")
		return nil
	}

	var temps []float64
	var pressures []float64
	var humidities []float64
	var winds []float64
	for _, place := range similarPlaces {
		if place.Temperature != nil {
			temps = append(temps, *place.Temperature)
		}
		if place.Humidity != nil {
			humidities = append(humidities, *place.Humidity)
		}
		if place.Pressure != nil {
			pressures = append(pressures, *place.Pressure)
		}
		if place.WindSpeed != nil {
			winds = append(winds, *place.WindSpeed)
		}
	}
	var avgTemp, avgPressure, avgHumidity, avgWind float64
	if len(temps) > 0 {avgTemp = Sum(temps) / float64(len(temps))}else {avgTemp = 0}
	if len(humidities) > 0 {avgHumidity = Sum(humidities) / float64(len(humidities))}else {avgHumidity = 0}
	if len(pressures) > 0 {avgPressure= Sum(pressures) / float64(len(pressures))}else {avgPressure = 0}
	if len(winds) > 0 {avgWind = Sum(winds) / float64(len(winds))}else {avgWind = 0}

	threshold := 0.65
	anomaly := map[string]any{
		"temperature": isAnomaly(temperature, avgTemp, threshold, true),
		"pressure":    isAnomaly(pressure, avgPressure, threshold, true),
		"humidity":    isAnomaly(humidity, avgHumidity, threshold, false),
		"wind_speed":  isAnomaly(windSpeed, avgWind, threshold, false),
		"averages": map[string]float64{
			"temperature": avgTemp,
			"pressure":    avgPressure,
			"humidity":    avgHumidity,
			"wind_speed":  avgWind,
		},
	}
	return anomaly

}

func getField(w APIWeather, field string) *float64 {
	switch field {
	case "temperature":
		return w.Temperature
	case "humidity":
		return w.Humidity
	case "pressure":
		return w.Pressure
	case "wind_speed":
		return w.WindSpeed
	default:
		return nil
	}
}

func CheckAnomalies(pool *pgxpool.Pool, ctx context.Context, data APIWeather) map[string]any {
	anomalies := compareWeather(pool, ctx, data)
	if anomalies != nil {
		var anomalies_to_save map[string]*float64 = make(map[string]*float64)
		var anomalies_data map[string]map[string]any = make(map[string]map[string]any)

		for key, is_anomaly := range anomalies {
			if is_anomaly == true && key != "averages" {
				anomaly_value := getField(data, key)
				if anomaly_value != nil {
					fmt.Printf("Аномалия: %s: %v! Среднее: %f\n", key, anomaly_value, anomalies["averages"].(map[string]float64)[key])

					switch key {
					case "temperature":
						anomalies_to_save["anomaly_temperature"] = anomaly_value
					case "pressure":
						anomalies_to_save["anomaly_pressure"] = anomaly_value
					case "humidity":
						anomalies_to_save["anomaly_humidity"] = anomaly_value
					case "wind_speed":
						anomalies_to_save["anomaly_wind_speed"] = anomaly_value
					}

					anomalies_data[key] = map[string]any{"value": anomaly_value, "avg": anomalies["averages"].(map[string]float64)[key]}
				}
			}
		}

		if len(anomalies_to_save) > 0 {
			// saved_anomaly := saveAnomaly(pool, ctx, anomalies_to_save, data.LocId, anomalies_data)
			result := map[string]any{
				"loc_id":            data.LocId,
				"anomalies_to_save": anomalies_to_save,
				"anomalies_data":    anomalies_data,
			}
			return result
		}
	}

	return nil
}