// Command api is the DevOops training backend.
//
// It takes a JSON job request from the frontend, writes config.json and jobset.json to S3, and
// submits the rendered JobSet to the local k3s management cluster, where MultiKueue dispatches
// it to whichever EKS worker admits it first.
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
	jobSettings = jobSettings.WithCheckpointImage(cfg.CheckpointImage)

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
	events := repository.NewEvents(db, cfg.EventsTable)

	jobs := service.NewJobService(service.Config{
		Namespace:     cfg.Namespace,
		Region:        cfg.Region,
		Bucket:        cfg.Bucket,
		ProgressTable: cfg.ProgressTable,
		Settings:      jobSettings,
		NewID:         newID,
	},
		repository.NewObjects(s3.NewFromConfig(awsCfg), cfg.Bucket),
		kube,
		repository.NewProgress(db, cfg.ProgressTable),
		events, // 실행 기록 읽기. Kafka로 옮기면 이 인자만 바꾼다.
		events, // 백엔드가 남기는 기록 쓰기
	)

	// Management-cluster steps (Kueue quota, MultiKueue dispatch, finish). Worker steps come from
	// the job-watcher on each EKS cluster.
	go timeline.NewTracker(kube, events, model.ManagerCluster).Run(ctx, 2*time.Second)

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

	slog.Info("listening", "addr", srv.Addr, "bucket", cfg.Bucket, "namespace", cfg.Namespace)
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
