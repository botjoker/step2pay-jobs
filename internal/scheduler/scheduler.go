package scheduler

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sambacrm/scheduler/internal/action"
	"github.com/sambacrm/scheduler/internal/models"
	"github.com/sambacrm/scheduler/internal/repository"
	"github.com/sambacrm/scheduler/internal/trigger"
)

type Scheduler struct {
	repo         *repository.JobRepo
	pool         *pgxpool.Pool
	pollInterval time.Duration
}

func New(pool *pgxpool.Pool, pollInterval time.Duration) *Scheduler {
	return &Scheduler{
		repo:         repository.NewJobRepo(pool),
		pool:         pool,
		pollInterval: pollInterval,
	}
}

func (s *Scheduler) Run(ctx context.Context) {
	log.Printf("[scheduler] started, poll interval=%s", s.pollInterval)
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	s.tick(ctx)

	for {
		select {
		case <-ticker.C:
			s.tick(ctx)
		case <-ctx.Done():
			log.Println("[scheduler] shutting down")
			return
		}
	}
}

func (s *Scheduler) tick(ctx context.Context) {
	jobs, err := s.repo.LoadDueJobs(ctx)
	if err != nil {
		log.Printf("[scheduler] error loading jobs: %v", err)
		return
	}
	log.Printf("[scheduler] tick: %d due jobs", len(jobs))
	for _, job := range jobs {
		go s.processJob(ctx, job)
	}
}

func (s *Scheduler) processJob(ctx context.Context, job models.SchedulerJob) {
	log.Printf("[scheduler] job %s (%s) start", job.ID, job.Name)

	trig, ok := trigger.Registry[job.TriggerType]
	if !ok {
		s.finish(ctx, job, "error", fmt.Sprintf("unknown trigger: %s", job.TriggerType), 0)
		return
	}
	shouldRun, err := trig.ShouldRun(ctx, job, s.pool)
	if err != nil {
		s.finish(ctx, job, "error", fmt.Sprintf("trigger error: %v", err), 0)
		return
	}
	if !shouldRun {
		s.finish(ctx, job, "skipped", "condition not met", 0)
		return
	}

	act, ok := action.Registry[job.ActionType]
	if !ok {
		s.finish(ctx, job, "error", fmt.Sprintf("unknown action: %s", job.ActionType), 0)
		return
	}
	count, err := act.Execute(ctx, job, s.pool)
	if err != nil {
		s.finish(ctx, job, "error", fmt.Sprintf("action error: %v", err), 0)
		return
	}
	s.finish(ctx, job, "success", fmt.Sprintf("affected %d records", count), count)
}

func (s *Scheduler) finish(ctx context.Context, job models.SchedulerJob, status, message string, count int) {
	log.Printf("[scheduler] job %s: %s — %s", job.ID, status, message)

	nextRun, err := trigger.NextRunForJob(job, s.pollInterval)
	if err != nil {
		log.Printf("[scheduler] warn: cannot compute next run: %v", err)
	}

	errMsg := ""
	if status == "error" {
		errMsg = message
	}
	if err := s.repo.UpdateAfterRun(ctx, job.ID, nextRun, status, errMsg); err != nil {
		log.Printf("[scheduler] warn: UpdateAfterRun: %v", err)
	}
	if err := s.repo.WriteLog(ctx, job.ID, status, message, count); err != nil {
		log.Printf("[scheduler] warn: WriteLog: %v", err)
	}
}
