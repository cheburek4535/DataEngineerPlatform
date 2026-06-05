package main

import (
	"context"
	analyze "dataengineerpolygon/analyze"
	pb "dataengineerpolygon/pb"
	"io"
	"log"
	"net"
	"net/http"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedLifeScoreServiceServer
}

func convertProtoToData(pbLoc *pb.LocationData) analyze.APIRequest {
    data := analyze.APIRequest{
        LocID: int(pbLoc.LocId),
    }
    
    // Конвертируем Air Quality
    for _, pbAq := range pbLoc.Aq {
        aq := analyze.AirQuality{
            Pm25: pbAq.Pm25,
            Pm10: pbAq.Pm10,
            NO2:  pbAq.No2,
            O3:   pbAq.O3,
            SO2:  pbAq.So2,
            CO:   pbAq.Co,
        }
        data.AQ = append(data.AQ, aq)
    }
    
    // Конвертируем Weather
    for _, pbW := range pbLoc.Weather {
        w := analyze.APIWeatherLS{
            Temperature: pbW.Temp,
            Pressure:    pbW.Pres,
            Humidity:    pbW.Hum,
            WindSpeed:   pbW.Wind,
        }
        data.Weather = append(data.Weather, w)
    }
    
    // Конвертируем Anomalies
    for _, pbA := range pbLoc.Anomalies {
        a := analyze.Anomaly{
            Temperature: pbA.Temp,
            Pressure:    pbA.Pres,
            Humidity:    pbA.Hum,
            WindSpeed:   pbA.Wind,
        }
        data.Anomalies = append(data.Anomalies, a)
    }
    
    return data
}

func (s *server) CalculateBatch(ctx context.Context, req *pb.BatchRequest) (*pb.BatchResponse, error) {
	var scores []*pb.LifeScoreResponse

	for _, loc := range req.Locations {
		data := convertProtoToData(loc)
		result := analyze.ProcessData(data)

		
		if result != nil {
			scores = append(scores, &pb.LifeScoreResponse{
				LocationId:      int64(result.LocationID),
				GeneralScore:    result.GeneralScore, // 
				AirQuality:      int32(result.AirQuality),
				WeatherQuality:  int32(result.WeatherQuality),
				AnomaliesDanger: int32(result.AnomaliesDanger),
			})
		}
	}
	return &pb.BatchResponse{Scores: scores}, nil
}

// Потоковая обработка - более эффективно для больших данных
func (s *server) CalculateLifeScore(stream pb.LifeScoreService_CalculateLifeScoreServer) error {
	for {
		data, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		
		locData := convertProtoToData(data)
		result := analyze.ProcessData(locData)
		
		if result != nil {
			score := &pb.LifeScoreResponse{
				LocationId:      int64(result.LocationID),
				GeneralScore:    result.GeneralScore, 
				AirQuality:      int32(result.AirQuality),
				WeatherQuality:  int32(result.WeatherQuality),
				AnomaliesDanger: int32(result.AnomaliesDanger),
			}
			if err := stream.Send(score); err != nil {
				return err
			}
		}
	}
}


func main() {
	pool := analyze.ConnectToPostgres()
	ctx := context.Background()
	defer pool.Close()

	// Запускаем gRPC в горутине
	go func() {
		lis, err := net.Listen("tcp", ":50051")
		if err != nil {
			log.Fatalf("gRPC listen error: %v", err)
		}
		s := grpc.NewServer(
			grpc.MaxRecvMsgSize(100*1024*1024),
			grpc.MaxSendMsgSize(100*1024*1024),
		)
		pb.RegisterLifeScoreServiceServer(s, &server{})
		log.Println("gRPC server listening on :50051")
		if err := s.Serve(lis); err != nil {
			log.Fatalf("gRPC serve error: %v", err)
		}
	}()

	// HTTP сервер
	r := gin.Default()
	r.POST("/weather/batch", func(c *gin.Context) {
		var batch []analyze.APIWeather
		if err := c.ShouldBindJSON(&batch); err != nil {
			c.JSON(http.StatusBadRequest, analyze.ErrorResponse{Error: err.Error()})
			return
		}
		var result []map[string]any
		for _, w := range batch {
			anomalies := analyze.CheckAnomalies(pool, ctx, w)
			if anomalies != nil {
				result = append(result, anomalies)
			}
		}
		c.JSON(http.StatusOK, gin.H{"processed": len(batch), "result": result})
	})

	log.Println("HTTP server listening on :8000")
	if err := r.Run(":8000"); err != nil {
		log.Fatal(err)
	}
}

