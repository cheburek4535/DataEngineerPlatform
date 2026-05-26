package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
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

func connectToPostgres() *pgxpool.Pool {
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

func checkAnomalies(pool *pgxpool.Pool, ctx context.Context, data APIWeather) map[string]any {
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

func main() {
	pool := connectToPostgres()
	ctx := context.Background()
	defer pool.Close()

	r := gin.Default()

	r.POST("/weather/batch", func(c *gin.Context) {
		var batch []APIWeather
		if err := c.ShouldBindJSON(&batch); err != nil {
			c.JSON(http.StatusBadRequest, ErrorResponse{Error: err.Error()})
			return
		}

		var result []map[string]any = make([]map[string]any, 0, len(batch))
		for _, w := range batch {
			anomalies := checkAnomalies(pool, ctx, w)
			if anomalies != nil {
				result = append(result, anomalies)
			}
		}
		c.JSON(http.StatusOK, gin.H{
			"processed": len(batch),
			"result":    result,
		})
	})
	if err := r.Run(":8000"); err != nil {
		log.Fatal(err)
	}
}

// func saveAnomaly(
// 	pool *pgxpool.Pool,
// 	ctx context.Context,
// 	anomalies map[string]*float64,
// 	locId int,
// 	data map[string]map[string]any,
// ) map[string]any {

// 	// Переменные для считывания данных из базы
// 	var lastID int
// 	var lastFoundAt time.Time
// 	var lastMetricsJSON []byte

// 	// 1. ПОЛУЧЕНИЕ ДАННЫХ: Берем только одну последнюю запись для этой локации
// 	err := pool.QueryRow(ctx,
// 		`SELECT id, found_at, metrics FROM Anomaly WHERE location_id = $1 ORDER BY found_at DESC LIMIT 1`,
// 		locId,
// 	).Scan(&lastID, &lastFoundAt, &lastMetricsJSON)

// 	hasOldAnomaly := true
// 	if err != nil {
// 		if err == pgx.ErrNoRows {
// 			hasOldAnomaly = false // Записей нет, это первая аномалия
// 		} else {
// 			log.Printf("Ошибка при запросе к БД: %v", err)
// 			return nil
// 		}
// 	}

// 	// Распаковываем старые метрики из JSON в map для сравнения
// 	lastMetrics := make(map[string]*float64)
// 	if hasOldAnomaly && len(lastMetricsJSON) > 0 {
// 		if err := json.Unmarshal(lastMetricsJSON, &lastMetrics); err != nil {
// 			log.Printf("Ошибка распаковки JSON старых метрик: %v", err)
// 		}
// 	}

// 	// 2.ЛОГИКА ПРОВЕРКИ: Нужно ли создавать новую запись?
// 	createNew := false
// 	if !hasOldAnomaly {
// 		createNew = true // Если старых записей нет — точно создаем новую
// 	} else {
// 		newAnomaliesCount := 0

// 		// Проверяем, изменились ли метрики
// 		for k, newValue := range anomalies {
// 			oldValue, exists := lastMetrics[k]

// 			// Если метрики не было раньше, или значения не совпадают
// 			if !exists || (newValue != nil && oldValue != nil && *newValue != *oldValue) || (newValue == nil || oldValue == nil) && newValue != oldValue {
// 				newAnomaliesCount++
// 			}
// 		}

// 		// Проверяем время: прошло ли больше 31 дня
// 		monthAgo := time.Now().UTC().AddDate(0, 0, -31)
// 		timeExpired := lastFoundAt.Before(monthAgo)

// 		if newAnomaliesCount > 0 || timeExpired {
// 			createNew = true
// 		}
// 	}

// 	// 3. СОХРАНЕНИЕ В БД: INSERT или UPDATE
// 	if createNew {
// 		// Кодируем текущие аномалии и доп. данные в JSON для записи в БД
// 		metricsBytes, err := json.Marshal(anomalies)
// 		if err != nil {
// 			log.Printf("Ошибка кодирования метрик в JSON: %v", err)
// 			return nil
// 		}

// 		additionalDataBytes, err := json.Marshal(data)
// 		if err != nil {
// 			log.Printf("Ошибка кодирования доп. данных в JSON: %v", err)
// 			return nil
// 		}

// 		now := time.Now().UTC()

// 		if hasOldAnomaly {
// 			// ОБНОВЛЕНИЕ: если аномалия та же, просто обновляем время и данные
// 			_, err = pool.Exec(ctx,
// 				`UPDATE Anomaly SET found_at = $1, metrics = $2, additional_data = $3 WHERE id = $4`,
// 				now, metricsBytes, additionalDataBytes, lastID,
// 			)
// 			if err != nil {
// 				log.Printf("Ошибка UPDATE в БД: %v", err)
// 				return nil
// 			}
// 		} else {
// 			// СОЗДАНИЕ: если это абсолютно новая аномалия
// 			_, err = pool.Exec(ctx,
// 				`INSERT INTO Anomaly (location_id, found_at, metrics, additional_data) VALUES ($1, $2, $3, $4)`,
// 				locId, now, metricsBytes, additionalDataBytes,
// 			)
// 			if err != nil {
// 				log.Printf("Ошибка INSERT в БД: %v", err)
// 				return nil
// 			}
// 		}

// 		// Возвращаем результат в виде карты
// 		return map[string]any{
// 			"status":      "success",
// 			"location_id": locId,
// 			"saved_at":    now,
// 			"is_new":      !hasOldAnomaly,
// 		}
// 	}

// 	// Если ничего не создавали и не обновляли
// 	return nil
// }
