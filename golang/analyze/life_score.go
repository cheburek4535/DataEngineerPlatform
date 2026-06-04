package analyze

type APIRequest struct {
	LocID     int          `json:"loc_id"`
	AQ        []AirQuality `json:"aq"`
	Weather   []APIWeatherLS `json:"weather"`
	Anomalies []Anomaly    `json:"anomalies"`
}

// Качество воздуха

type AirQuality struct {
	Pm25 *float64 `json:"pm25"`
	Pm10 *float64 `json:"pm10"`
	NO2  *float64 `json:"no2"`
	O3   *float64 `json:"o3"`
	SO2  *float64 `json:"so2"`
	CO   *float64 `json:"co"`
}

type AvgAirQuality struct {
	Pm25 *float64
	Pm10 *float64
	NO2  *float64
	O3   *float64
	SO2  *float64
	CO   *float64
}

type AirQualityLevel int

const (
	LevelGood AirQualityLevel = iota
	LevelModerate
	LevelUnhealthySensitive
	LevelUnhealthy
	LevelVeryUnhealthy
	LevelHazardous
)

// Формат: (хорошо, удовлетворительно, вредно для чувствительных, вредно, очень вредно)
var (
	PM25Thresholds = []float64{12, 35, 55, 150, 250}       // µg/m³
	PM10Thresholds = []float64{20, 50, 100, 200, 350}      // µg/m³
	NO2Thresholds  = []float64{0.02, 0.05, 0.1, 0.2, 0.5}  // ppm
	O3Thresholds   = []float64{0.05, 0.07, 0.1, 0.15, 0.2} // ppm
	SO2Thresholds  = []float64{0.02, 0.05, 0.1, 0.2, 0.5}  // ppm
	COThresholds   = []float64{4, 9, 15, 30, 50}           // ppm
)

func CalculateAirQualityLevel(data AvgAirQuality) *AirQualityLevel {
	var maxLevel AirQualityLevel = -1
	hasData := false

	if data.Pm25 != nil {
		level := levelForValue(*data.Pm25, PM25Thresholds)
		if level > maxLevel {
			maxLevel = level
		}
		hasData = true
	}
	if data.Pm10 != nil {
		level := levelForValue(*data.Pm10, PM10Thresholds)
		if level > maxLevel {
			maxLevel = level
		}
		hasData = true
	}
	if data.NO2 != nil {
		level := levelForValue(*data.NO2, NO2Thresholds)
		if level > maxLevel {
			maxLevel = level
		}
		hasData = true
	}
	if data.O3 != nil {
		level := levelForValue(*data.O3, O3Thresholds)
		if level > maxLevel {
			maxLevel = level
		}
		hasData = true
	}
	if data.SO2 != nil {
		level := levelForValue(*data.SO2, SO2Thresholds)
		if level > maxLevel {
			maxLevel = level
		}
		hasData = true
	}
	if data.CO != nil {
		level := levelForValue(*data.CO, COThresholds)
		if level > maxLevel {
			maxLevel = level
		}
		hasData = true
	}

	if !hasData {
		return nil
	}

	result := maxLevel
	return &result
}

func levelForValue(value float64, thresholds []float64) AirQualityLevel {
	if value <= thresholds[0] {
		return LevelGood
	} else if value <= thresholds[1] {
		return LevelModerate
	} else if value <= thresholds[2] {
		return LevelUnhealthySensitive
	} else if value <= thresholds[3] {
		return LevelUnhealthy
	} else if value <= thresholds[4] {
		return LevelVeryUnhealthy
	}
	return LevelHazardous
}

func calculateAverageAQ(data []AirQuality) *AvgAirQuality {
	var sumPm25, sumPm10, sumNo2, sumO3, sumSo2, sumCo float64
	var countPm25, countPm10, countNo2, countO3, countSo2, countCo int

	for _, item := range data {
		if item.Pm25 != nil {
			sumPm25 += *item.Pm25
			countPm25++
		}
		if item.Pm10 != nil {
			sumPm10 += *item.Pm10
			countPm10++
		}
		if item.NO2 != nil {
			sumNo2 += *item.NO2
			countNo2++
		}
		if item.O3 != nil {
			sumO3 += *item.O3
			countO3++
		}
		if item.SO2 != nil {
			sumSo2 += *item.SO2
			countSo2++
		}
		if item.CO != nil {
			sumCo += *item.CO
			countCo++
		}
	}

	avg := &AvgAirQuality{}

	if countPm25 > 0 {
		val := sumPm25 / float64(countPm25)
		avg.Pm25 = &val
	}
	if countPm10 > 0 {
		val := sumPm10 / float64(countPm10)
		avg.Pm10 = &val
	}
	if countNo2 > 0 {
		val := sumNo2 / float64(countNo2)
		avg.NO2 = &val
	}
	if countO3 > 0 {
		val := sumO3 / float64(countO3)
		avg.O3 = &val
	}
	if countSo2 > 0 {
		val := sumSo2 / float64(countSo2)
		avg.SO2 = &val
	}
	if countCo > 0 {
		val := sumCo / float64(countCo)
		avg.CO = &val
	}

	return avg
}

func analyzeAQ(data []AirQuality) *AirQualityLevel {
	if len(data) == 0 {
		return nil
	}
	avg_aq := calculateAverageAQ(data)
	if avg_aq == nil {
		return nil
	}
	result := CalculateAirQualityLevel(*avg_aq)

	return result
}

// Погода

type APIWeatherLS struct {
	Temperature *float64 `json:"temp"`
	Pressure    *float64 `json:"pres"`
	Humidity    *float64 `json:"hum"`
	WindSpeed   *float64 `json:"wind"`
}

type AvgWeather struct {
	Temperature *float64
	Pressure    *float64
	Humidity    *float64
	WindSpeed   *float64
}

type Range struct {
	Min float64
	Max float64
}

type WeatherMetric []Range
type WeatherLevel int

var (
	// Температура (°C): Идеал ~20. В обе стороны идет ухудшение.
	tempThresholds = WeatherMetric{
		{Min: 18.0, Max: 23.0},  // Хорошо
		{Min: 10.0, Max: 27.0},  // Удовлетворительно
		{Min: 0.0, Max: 33.0},   // Вредно для чувствительных (минус на улице или жара)
		{Min: -15.0, Max: 40.0}, // Вредно
		{Min: -50.0, Max: 60.0}, // Очень вредно (экстремальный мороз / жара)
	}

	// Влажность (%): Идеал ~50. Вредна как сухость, так и сырость.
	humidityThresholds = WeatherMetric{
		{Min: 40.0, Max: 60.0}, // Хорошо
		{Min: 30.0, Max: 70.0}, // Удовлетворительно
		{Min: 20.0, Max: 80.0}, // Вредно для чувствительных
		{Min: 10.0, Max: 90.0}, // Вредно
		{Min: 0.0, Max: 100.0}, // Очень вредно
	}

	// Давление (гПа): Идеал ~1013.25. Вредно и низкое (циклон), и высокое (антициклон).
	pressureThresholds = WeatherMetric{
		{Min: 1008.0, Max: 1018.0}, // Хорошо
		{Min: 1000.0, Max: 1025.0}, // Удовлетворительно
		{Min: 990.0, Max: 1035.0},  // Вредно для чувствительных
		{Min: 970.0, Max: 1050.0},  // Вредно
		{Min: 900.0, Max: 1100.0},  // Очень вредно
	}

	windThresholds = WeatherMetric{
		{Min: 0.0, Max: 5.0},
		{Min: 0.0, Max: 8.0},
		{Min: 0.0, Max: 14.0},
		{Min: 0.0, Max: 20.0},
		{Min: 0.0, Max: 60.0},
	}
)

// Функция определяет индекс состояния (0 - хорошо, 4 - очень вредно)
func getStatusIndex(val float64, metric WeatherMetric) WeatherLevel {
	for i, r := range metric {
		if val >= r.Min && val <= r.Max {
			return WeatherLevel(i)
		}
	}
	return 4 // Если вышло за все рамки — это очень вредно
}

func calculateAverageWeather(data []APIWeatherLS) *AvgWeather {
	var sumTemperature, sumHumidity, sumPressure, sumWindSpeed float64
	var countTemperature, countHumidity, countPressure, countWindSpeed int

	for _, item := range data {
		if item.Temperature != nil {
			sumTemperature += *item.Temperature
			countTemperature++
		}
		if item.Humidity != nil {
			sumHumidity += *item.Humidity
			countHumidity++
		}
		if item.Pressure != nil {
			sumPressure += *item.Pressure
			countPressure++
		}
		if item.WindSpeed != nil {
			sumWindSpeed += *item.WindSpeed
			countWindSpeed++
		}
	}

	avg := &AvgWeather{}

	if countTemperature > 0 {
		val := sumTemperature / float64(countTemperature)
		avg.Temperature = &val
	}
	if countPressure > 0 {
		val := sumPressure / float64(countPressure)
		avg.Pressure = &val
	}
	if countHumidity > 0 {
		val := sumHumidity / float64(countHumidity)
		avg.Humidity = &val
	}
	if countWindSpeed > 0 {
		val := sumWindSpeed / float64(countWindSpeed)
		avg.WindSpeed = &val
	}

	return avg
}

func analyzeWeather(data []APIWeatherLS) *WeatherLevel {
	if len(data) == 0 {
		return nil
	}
	avg_aq := calculateAverageWeather(data)
	if avg_aq == nil {
		return nil
	}

	var maxLevel WeatherLevel = -1
	hasData := false

	if avg_aq.Temperature != nil {
		level := getStatusIndex(*avg_aq.Temperature, tempThresholds)
		if level > maxLevel {
			maxLevel = level
		}
		hasData = true
	}
	if avg_aq.Pressure != nil {
		level := getStatusIndex(*avg_aq.Pressure, pressureThresholds)
		if level > maxLevel {
			maxLevel = level
		}
		hasData = true
	}
	if avg_aq.Humidity != nil {
		level := getStatusIndex(*avg_aq.Humidity, humidityThresholds)
		if level > maxLevel {
			maxLevel = level
		}
		hasData = true
	}
	if avg_aq.WindSpeed != nil {
		level := getStatusIndex(*avg_aq.WindSpeed, windThresholds)
		if level > maxLevel {
			maxLevel = level
		}
		hasData = true
	}

	if !hasData {
		return nil
	}
	return &maxLevel

}

// Аномалии

type Anomaly struct {
	Temperature *float64 `json:"temp"`
	Pressure    *float64 `json:"pres"`
	Humidity    *float64 `json:"hum"`
	WindSpeed   *float64 `json:"wind"`
}
type AnomalyLevel int

func processAnomalies(data []Anomaly) AnomalyLevel {
	var level int
	if len(data) == 0 {
		level = 0
	} else if len(data) <= 5 {
		level = len(data) - 1
	} else {
		level = 4
	}
	return AnomalyLevel(level)

}

// Общая обработка
type AnalyzeResult struct {
	LocationID      int
	GeneralScore    float64
	AirQuality      int
	WeatherQuality  int
	AnomaliesDanger int
}


func ProcessData(data APIRequest) *AnalyzeResult{
	aqLevel := analyzeAQ(data.AQ)
	weatherLevel := analyzeWeather(data.Weather)
	anomaliesLevel := processAnomalies(data.Anomalies)

	if aqLevel == nil || weatherLevel == nil {
		return nil
	}
	generalLevel := float64((int(*aqLevel) + int(*weatherLevel) + int(anomaliesLevel))) / 3.0
	return &AnalyzeResult{
		LocationID:      int(data.LocID),
		GeneralScore:    float64(generalLevel),
		AirQuality:      int(*aqLevel),
		WeatherQuality:  int(*weatherLevel),
		AnomaliesDanger: int(anomaliesLevel),
	}

}
