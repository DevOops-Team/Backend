// Command api is the DevOops training backend.
//
// It takes a multipart job submission from the frontend (settings + a Hugging Face model folder +
// jsonl training data), streams the files to S3, writes config.json and jobset.json, creates the
// fencing item and submits the JobSet to the management cluster, where Kueue · MultiKueue and the
// dispatcher send it to an EKS worker. It reports job.submitted to Kafka.
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

	jobs := service.NewJobService(service.Config{
		Namespace:      cfg.Namespace,
		Region:         cfg.Region,
		Bucket:         cfg.Bucket,
		FencingTable:   cfg.FencingTable,
		ProgressTable:  cfg.ProgressTable,
		KafkaBootstrap: cfg.KafkaBootstrap,
		KafkaTopic:     cfg.KafkaTopic,
		Settings:       jobSettings,
		NewID:          newID,
	}, service.Deps{
		Objects:   repository.NewObjects(s3.NewFromConfig(awsCfg), cfg.Bucket),
		Scheduler: kube,
		Progress:  repository.NewProgress(db, cfg.ProgressTable),
		Fencing:   repository.NewFencing(db, cfg.FencingTable),
		Lifecycle: lifecycle,
		Events:    timelineStore,
		Record:    timelineStore,
	})

	// Management-cluster steps (Kueue quota, MultiKueue dispatch, finish) for the job timeline.
	go timeline.NewTracker(kube, timelineStore, model.ManagerCluster).Run(ctx, 2*time.Second)

	gin.SetMode(gin.ReleaseMode)
	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           handler.NewRouter(jobs, cfg.AllowedOrigin),
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
		"trainer", jobSettings.Images.Trainer, "checkpoint", jobSettings.Images.Checkpoint, "kafka", cfg.KafkaBootstrap)
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
