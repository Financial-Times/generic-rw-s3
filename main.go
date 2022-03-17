package main

import (
	"net"
	"net/http"
	"os"
	"time"

	"github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/message-queue-gonsumer/consumer"
	"github.com/aws/aws-sdk-go/aws"
	credentials "github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gorilla/mux"
	"github.com/ivan-p-nikolov/jeager-service-example/fttracing"
	cli "github.com/jawher/mow.cli"

	"github.com/Financial-Times/generic-rw-s3/service"
)

const (
	spareWorkers = 10 // Workers for things like health check, gtg, count, etc...
)

func main() {
	app := cli.App("generic-rw-s3", "A RESTful API for writing data to S3")

	appName := app.String(cli.StringOpt{
		Name:   "app-name",
		Desc:   "Application name",
		EnvVar: "APP_NAME",
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

	sourceAddresses := app.Strings(cli.StringsOpt{
		Name:   "source-addresses",
		Value:  []string{},
		Desc:   "Addresses used by the queue consumer to connect to the queue",
		EnvVar: "SRC_ADDR",
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

	sourceConcurrentProcessing := app.Bool(cli.BoolOpt{
		Name:   "source-concurrent-processing",
		Value:  false,
		Desc:   "Whether the consumer uses concurrent processing for the messages",
		EnvVar: "SRC_CONCURRENT_PROCESSING",
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

		qConf := consumer.QueueConfig{
			Addrs:                *sourceAddresses,
			Group:                *sourceGroup,
			Topic:                *sourceTopic,
			ConcurrentProcessing: *sourceConcurrentProcessing,
		}
		runServer(*appName, *port, *resourcePath, *awsRegion, *bucketName, *bucketPrefix, *wrkSize, qConf, *onlyUpdatesEnabled, *requestLoggingEnabled)
	}

	logger.InitLogger(*appName, *logLevel)
	logger.Infof("Application started with args %s", os.Args)
	app.Run(os.Args)
}

func runServer(appName string, port string, resourcePath string, awsRegion string, bucketName string, bucketPrefix string, wrks int, qConf consumer.QueueConfig, onlyUpdatesEnabled bool, requestLoggingEnabled bool) {
	shutdown, err := fttracing.InitTracing(appName)
	if err != nil {
		logger.WithError(err).Warn("failed to init tracing")
	} else {
		defer shutdown()
	}
	hc := http.Client{
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
	hc = fttracing.NewHTTPClient(hc.Transport)
	cfg := &aws.Config{
		Region:     aws.String(awsRegion),
		MaxRetries: aws.Int(1),
		HTTPClient: &hc,
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
		logger.Fatalf("Failed to create AWS session: %v", err)
	}
	svc := s3.New(sess)

	w := service.NewS3Writer(svc, bucketName, bucketPrefix, onlyUpdatesEnabled)
	r := service.NewS3Reader(svc, bucketName, bucketPrefix, int16(wrks))

	wh := service.NewWriterHandler(w, r)
	rh := service.NewReaderHandler(r)

	servicesRouter := mux.NewRouter()

	service.AddAdminHandlers(servicesRouter, svc, bucketName, appName, requestLoggingEnabled)
	service.Handlers(servicesRouter, wh, rh, resourcePath)
	fttracing.AddTelemetry(servicesRouter, appName)
	qp := service.NewQProcessor(w)

	logger.Infof("listening on %v", port)

	if qConf.Topic != "" {
		c := consumer.NewConsumer(qConf, qp.ProcessMsg, &hc)
		go c.Start()
		defer c.Stop()
	}
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		logger.Fatalf("Unable to start server: %v", err)
	}

}
