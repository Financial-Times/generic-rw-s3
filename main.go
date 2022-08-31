package main

import (
	"net"
	"net/http"
	"os"
	"time"

	"github.com/Financial-Times/generic-rw-s3/service"
	"github.com/Financial-Times/go-logger/v2"
	"github.com/Financial-Times/kafka-client-go/v3"
	"github.com/aws/aws-sdk-go/aws"
	credentials "github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gorilla/mux"
	cli "github.com/jawher/mow.cli"
)

const (
	spareWorkers   = 10 // Workers for things like health check, gtg, count, etc...
	serviceName    = "generic-rw-s3"
	appDescription = "A RESTful API for writing data to S3"
)

func main() {
	app := cli.App(serviceName, appDescription)

	appName := app.String(cli.StringOpt{
		Name:   "app-name",
		Desc:   "Application name",
		EnvVar: "APP_NAME",
	})
	appSystemCode := app.String(cli.StringOpt{
		Name:   "appSystemCode",
		Desc:   "Application systemCode",
		EnvVar: "APP_SYSTEM_CODE",
	})

	port := app.String(cli.StringOpt{
		Name:   "port",
		Value:  "8080",
		Desc:   "Port to listen on",
		EnvVar: "APP_PORT",
	})

	resourcePath := app.String(cli.StringOpt{
		Name:   "resourcePath",
		Value:  "",
		Desc:   "Request path parameter to identify a resource, e.g. /concepts",
		EnvVar: "RESOURCE_PATH",
	})

	awsRegion := app.String(cli.StringOpt{
		Name:   "awsRegion",
		Value:  "eu-west-1",
		Desc:   "AWS Region to connect to",
		EnvVar: "AWS_REGION",
	})

	bucketName := app.String(cli.StringOpt{
		Name:   "bucketName",
		Value:  "",
		Desc:   "Bucket name to upload things to",
		EnvVar: "BUCKET_NAME",
	})

	bucketPrefix := app.String(cli.StringOpt{
		Name:   "bucketPrefix",
		Value:  "",
		Desc:   "Prefix for content going into S3 bucket",
		EnvVar: "BUCKET_PREFIX",
	})

	wrkSize := app.Int(cli.IntOpt{
		Name:   "workers",
		Value:  10,
		Desc:   "Number of workers to use when batch downloading",
		EnvVar: "WORKERS",
	})

	kafkaAddress := app.String(cli.StringOpt{
		Name:   "kafkaAddress",
		Value:  "",
		Desc:   "Address to connect to kafka",
		EnvVar: "KAFKA_ADDRESS",
	})

	consumerLagTolerance := app.Int(cli.IntOpt{
		Name:   "consumerLagTolerance",
		Value:  120,
		Desc:   "Kafka lag tolerance",
		EnvVar: "KAFKA_LAG_TOLERANCE",
	})

	sourceGroup := app.String(cli.StringOpt{
		Name:   "source-group",
		Value:  "",
		Desc:   "Group used to read the messages from the queue",
		EnvVar: "SRC_GROUP",
	})

	sourceTopic := app.String(cli.StringOpt{
		Name:   "source-topic",
		Value:  "",
		Desc:   "The topic to read the messages from",
		EnvVar: "SRC_TOPIC",
	})

	logLevel := app.String(cli.StringOpt{
		Name:   "log-level",
		Value:  "info",
		Desc:   "Level of required logging",
		EnvVar: "LOG_LEVEL",
	})

	onlyUpdatesEnabled := app.Bool(cli.BoolOpt{
		Name:   "only-updates-enabled",
		Value:  false,
		Desc:   "When enabled app will only write to s3 when concept has changed since last write",
		EnvVar: "ONLY_UPDATES_ENABLED",
	})
	requestLoggingEnabled := app.Bool(cli.BoolOpt{
		Name:   "requestLoggingEnabled",
		Value:  false,
		Desc:   "Whether http request logging is enabled",
		EnvVar: "REQUEST_LOGGING_ENABLED",
	})

	app.Action = func() {

		log := logger.NewUPPLogger(serviceName, *logLevel)

		consumerConfig := kafka.ConsumerConfig{
			BrokersConnectionString: *kafkaAddress,
			ConsumerGroup:           *sourceGroup,
			ConnectionRetryInterval: time.Minute,
		}
		runServer(*appName, *port, *appSystemCode, *resourcePath, *awsRegion, *bucketName, *bucketPrefix, *wrkSize, sourceTopic, consumerLagTolerance, consumerConfig, *onlyUpdatesEnabled, *requestLoggingEnabled, log)
	}

	app.Run(os.Args)
}

func runServer(appName string, port string, appSystemCode string, resourcePath string, awsRegion string, bucketName string, bucketPrefix string, wrks int, readTopic *string, consumerLagTolerance *int, qConf kafka.ConsumerConfig, onlyUpdatesEnabled bool, requestLoggingEnabled bool, log *logger.UPPLogger) {
	hc := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          wrks + spareWorkers,
			IdleConnTimeout:       90 * time.Second,
			MaxIdleConnsPerHost:   wrks + spareWorkers,
			TLSHandshakeTimeout:   3 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}

	cfg := &aws.Config{
		Region:     aws.String(awsRegion),
		MaxRetries: aws.Int(1),
		HTTPClient: hc,
	}

	if os.Getenv("ENV") == "local" {
		endpoint := os.Getenv("S3_ENDPOINT")
		if endpoint == "" {
			endpoint = "http://localhost:8080"
		}

		cfg.Credentials = credentials.NewStaticCredentials("id", "secret", "token")
		cfg.Endpoint = aws.String(endpoint)
		cfg.DisableSSL = aws.Bool(true)
		cfg.S3ForcePathStyle = aws.Bool(true)
	}

	sess, err := session.NewSession(cfg)
	if err != nil {
		log.WithError(err).Fatal("Failed to create AWS session")
	}
	svc := s3.New(sess)

	w := service.NewS3Writer(svc, bucketName, bucketPrefix, onlyUpdatesEnabled, log)
	r := service.NewS3Reader(svc, bucketName, bucketPrefix, int16(wrks), log)

	wh := service.NewWriterHandler(w, r, log)
	rh := service.NewReaderHandler(r, log)

	servicesRouter := mux.NewRouter()

	service.Handlers(servicesRouter, wh, rh, resourcePath)

	qp := service.NewQProcessor(w, log)

	log.Infof("listening on %v", port)

	var consumer *kafka.Consumer
	if readTopic != nil {
		topics := []*kafka.Topic{kafka.NewTopic(*readTopic, kafka.WithLagTolerance(int64(*consumerLagTolerance)))}

		consumer = kafka.NewConsumer(qConf, topics, log)
		go consumer.Start(qp.ProcessMsg)
		defer consumer.Close()
	}
	service.AddAdminHandlers(servicesRouter, svc, bucketName, appName, appSystemCode, requestLoggingEnabled, log, consumer)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.WithError(err).Fatal("Unable to start server.")
	}

}
