package main

import (
	"github.com/Financial-Times/base-ft-rw-app-go/baseftrwapp"
	"github.com/Financial-Times/generic-rw-s3/service"
	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
	"net/http"
	"os"
)

func main() {
	app := cli.App("generic-rw-s3", "A RESTful API for writing data to S3")

	port := app.String(cli.StringOpt{
		Name:   "port",
		Value:  "8080",
		Desc:   "Port to listen on",
		EnvVar: "APP_PORT",
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

	graphiteTCPAddress := app.String(cli.StringOpt{
		Name:   "graphiteTCPAddress",
		Value:  "",
		Desc:   "Graphite TCP address, e.g. graphite.ft.com:2003. Leave as default if you do NOT want to output to graphite (e.g. if running locally",
		EnvVar: "GRAPHITE_ADDRESS",
	})

	graphitePrefix := app.String(cli.StringOpt{
		Name:   "graphitePrefix",
		Value:  "",
		Desc:   "Prefix to use. Should start with content, include the environment, and the host name. e.g. coco.pre-prod.service-name.1 or content.test.people.rw.service-name.ftaps58938-law1a-eu-t",
		EnvVar: "GRAPHITE_PREFIX",
	})

	logMetrics := app.Bool(cli.BoolOpt{
		Name:   "logMetrics",
		Value:  false,
		Desc:   "Whether to log metrics. Set to true if running locally and you want metrics output",
		EnvVar: "LOG_METRICS",
	})

	app.Action = func() {
		baseftrwapp.OutputMetricsIfRequired(*graphiteTCPAddress, *graphitePrefix, *logMetrics)
		runServer(*port, *awsRegion, *bucketName, *bucketPrefix, *wrkSize)
	}
	log.SetLevel(log.InfoLevel)
	log.Infof("Application started with args %s", os.Args)
	app.Run(os.Args)
}

func runServer(port string, awsRegion string, bucketName string, bucketPrefix string, wrks int) {
	sess, err := session.NewSession(
		&aws.Config{
			Region:     aws.String(awsRegion),
			MaxRetries: aws.Int(1),
		})
	if err != nil {
		log.Fatalf("Failed to create AWS session: %v", err)
	}
	svc := s3.New(sess)

	w := service.NewS3Writer(svc, bucketName, bucketPrefix)
	r := service.NewS3Reader(svc, bucketName, bucketPrefix, int16(wrks))

	wh := service.NewWriterHandler(w, r)
	rh := service.NewReaderHandler(r)

	servicesRouter := mux.NewRouter()
	service.Handlers(servicesRouter, wh, rh)
	service.AddAdminHandlers(servicesRouter, svc, bucketName, w, r)

	log.Infof("listening on %v", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Unable to start server: %v", err)
	}

}
