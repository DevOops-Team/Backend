// Command api is the DevOops training backend.
//
// It takes a multipart job submission from the frontend (settings + a Hugging Face model folder +
// jsonl training data), streams the files to S3, writes config.json and jobset.json, creates the
// fencing item and submits the JobSet to the management cluster, where Kueue · MultiKueue and the
// dispatcher send it to an EKS worker. It reports job.submitted to Kafka; the collector stores every
// job.lifecycle event in the data database, which the backend reads for the screens' status, progress
// and timeline.
//
// main only wires dependencies: config → repository → service → handler.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gin-gonic/gin"

	"github.com/DevOops-Team/Backend/internal/config"
	"github.com/DevOops-Team/Backend/internal/handler"
	"github.com/DevOops-Team/Backend/internal/model"
	"github.com/DevOops-Team/Backend/internal/repository"
	"github.com/DevOops-Team/Backend/internal/service"
	"github.com/DevOops-Team/Backend/internal/service/timeline"
	"github.com/DevOops-Team/Backend/internal/settings"
)

func main() {
	if err := run(); err != nil {
		slog.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return err
	}

	jobSettings, err := settings.Load(cfg.SettingsPath)
	if err != nil {
		return err
	}
	jobSettings = jobSettings.WithImages(cfg.TrainerImage, cfg.CheckpointImage)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return err
	}
	kube, err := repository.NewKube(cfg.Kubeconfig, cfg.KubeContext, cfg.Namespace)
	if err != nil {
		return err
	}
	db := dynamodb.NewFromConfig(awsCfg)
	lifecycle, err := repository.NewLifecycle(cfg.KafkaBootstrap, cfg.KafkaTopic)
	if err != nil {
		return err
	}
	defer lifecycle.Close()
	timelineStore := repository.NewTimeline(500)
	records, err := repository.NewJobRecords(ctx, cfg.DatabaseHost)
	if err != nil {
		return err
	}
	defer records.Close()

	fencing := repository.NewFencing(db, cfg.FencingTable)
	jobs := service.NewJobService(service.Config{
		Namespace:      cfg.Namespace,
		Region:         cfg.Region,
		Bucket:         cfg.Bucket,
		FencingTable:   cfg.FencingTable,
		KafkaBootstrap: cfg.KafkaBootstrap,
		KafkaTopic:     cfg.KafkaTopic,
		Settings:       jobSettings,
		NewID:          newID,
	}, service.Deps{
		Objects:   repository.NewObjects(s3.NewFromConfig(awsCfg), cfg.Bucket),
		Scheduler: kube,
		Records:   records,
		Fencing:   fencing,
		Lifecycle: lifecycle,
		Events:    timelineStore,
		Record:    timelineStore,
	})

	// Recovery controller lookup (GPU-119). No placement source is connected yet: the worker nodes of a job
	// are on the EKS workers, which the backend does not read, so the lookup answers 503 until one is.
	recovery := service.NewRecoveryService(cfg.Bucket, jobs, kube, fencing, service.NoPlacement{})

	// Management-cluster steps (Kueue quota, MultiKueue dispatch, finish) for the job timeline.
	go timeline.NewTracker(kube, timelineStore, model.ManagerCluster).Run(ctx, 2*time.Second)

	gin.SetMode(gin.ReleaseMode)
	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           handler.NewRouter(jobs, cfg.AllowedOrigin, handler.Internal{Recovery: recovery, Token: cfg.RecoveryAPIToken}),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdown); err != nil {
			slog.Warn("shutdown", "err", err)
		}
	}()

	slog.Info("listening", "addr", srv.Addr, "bucket", cfg.Bucket, "namespace", cfg.Namespace,
		"trainer", jobSettings.Images.Trainer, "checkpoint", jobSettings.Images.Checkpoint, "kafka", cfg.KafkaBootstrap,
		"database", cfg.DatabaseHost)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// newID returns 8 lowercase hex chars: short, and valid in Kubernetes names and S3 keys.
func newID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b) // crypto/rand.Read never returns an error
	return hex.EncodeToString(b)
}
