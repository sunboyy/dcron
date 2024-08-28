package dcron

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

const pollingInterval = time.Minute

var Parser = cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

type JobBuilder func(params JobParameters) (cron.Job, error)

type Dcron struct {
	jobBuilders           map[string]JobBuilder
	jobScheduleRepository JobScheduleRepository
	cron                  *cron.Cron
	instanceID            string
	running               bool
	runningMu             sync.Mutex
	updateMu              sync.Mutex
	logger                cron.Logger
	scheduleEntryID       map[int64]cron.EntryID
}

func New(jobScheduleRepository JobScheduleRepository) *Dcron {
	dcron := &Dcron{
		jobBuilders:           make(map[string]JobBuilder),
		jobScheduleRepository: jobScheduleRepository,
		cron:                  cron.New(),
		instanceID:            generateInstanceID(),
		logger:                cron.VerbosePrintfLogger(log.New(os.Stdout, "[DCRON] ", log.LstdFlags)),
		scheduleEntryID:       make(map[int64]cron.EntryID),
	}

	dcron.cron.Schedule(cron.Every(pollingInterval), cron.FuncJob(dcron.pollingUpdate))
	return dcron
}

func generateInstanceID() string {
	randomBytes := make([]byte, 8)
	_, _ = rand.Read(randomBytes)
	return hex.EncodeToString(randomBytes)
}

func (s *Dcron) RegisterJobBuilder(name string, builder JobBuilder) {
	s.updateMu.Lock()
	defer s.updateMu.Unlock()
	s.jobBuilders[name] = builder
}

func (s *Dcron) Start() {
	s.runningMu.Lock()
	defer s.runningMu.Unlock()

	if s.running {
		return
	}

	s.pollingUpdate()
	s.cron.Start()
	s.running = true

	s.logger.Info("Started", "instanceID", s.instanceID)
}

func (s *Dcron) pollingUpdate() {
	s.updateMu.Lock()
	defer s.updateMu.Unlock()

	s.logger.Info("Polling for updates")
	newJobSchedules, err := s.jobScheduleRepository.FindAll()
	if err != nil {
		s.logger.Error(err, "Failed to fetch job schedules")
		return
	}

	// Create a set of new job schedule IDs for quick lookup
	newJobScheduleIDSet := make(map[int64]struct{}, len(newJobSchedules))
	for _, jobSchedule := range newJobSchedules {
		newJobScheduleIDSet[jobSchedule.ID] = struct{}{}
	}

	// Unschedule jobs that are no longer present
	for id := range s.scheduleEntryID {
		if _, exists := newJobScheduleIDSet[id]; !exists {
			s.unschedule(id)
		}
	}

	// Schedule new jobs
	for _, jobSchedule := range newJobSchedules {
		s.scheduleJob(jobSchedule)
	}
}

func (s *Dcron) scheduleJob(jobSchedule JobSchedule) {
	if _, exists := s.scheduleEntryID[jobSchedule.ID]; exists {
		return // Already scheduled
	}

	schedule, err := Parser.Parse(jobSchedule.CronExpression)
	if err != nil {
		s.logger.Error(err, "Failed to parse cron expression",
			"cronExpression", jobSchedule.CronExpression)
		return
	}

	job, err := s.buildJob(jobSchedule.JobName, jobSchedule.Params)
	if err != nil {
		s.logger.Error(err, "Failed to build job", "jobName", jobSchedule.JobName)
		return
	}

	entryID := s.cron.Schedule(schedule,
		newDistributedJob(s.jobScheduleRepository, jobSchedule, s.instanceID, job))
	s.scheduleEntryID[jobSchedule.ID] = entryID

	s.logger.Info("Scheduled job", "key", jobSchedule.Key, "cronExpression", jobSchedule.CronExpression)
}

func (s *Dcron) buildJob(name string, params JobParameters) (cron.Job, error) {
	builder, exists := s.jobBuilders[name]
	if !exists {
		return nil, errors.New("no job builder found")
	}

	job, err := builder(params)
	if err != nil {
		return nil, fmt.Errorf("build job: %w", err)
	}

	return job, nil
}

func (s *Dcron) unschedule(id int64) {
	if entryID, exists := s.scheduleEntryID[id]; exists {
		s.cron.Remove(entryID)
		delete(s.scheduleEntryID, id)
		s.logger.Info("Unscheduled job", "id", id)
	}
}

func (s *Dcron) IsScheduled(key string) (bool, error) {
	jobSchedule, err := s.jobScheduleRepository.FindByKey(key)
	if err != nil {
		return false, fmt.Errorf("find job schedule by key: %w", err)
	}

	return jobSchedule != nil, nil
}

func (s *Dcron) Schedule(key, spec, jobName string, params JobParameters) error {
	if _, err := Parser.Parse(spec); err != nil {
		return fmt.Errorf("failed to parse cron expression %s: %w", spec, err)
	}

	s.updateMu.Lock()
	defer s.updateMu.Unlock()

	jobSchedule, err := s.jobScheduleRepository.Insert(key, spec, jobName, params)
	if err != nil {
		return err
	}

	if s.running {
		s.scheduleJob(jobSchedule)
	}

	return nil
}

func (s *Dcron) Unschedule(key string) error {
	jobSchedule, err := s.jobScheduleRepository.FindByKey(key)
	if err != nil {
		return err
	}

	s.updateMu.Lock()
	defer s.updateMu.Unlock()

	s.unschedule(jobSchedule.ID)
	return s.jobScheduleRepository.DeleteByKey(key)
}

func (s *Dcron) Reschedule(key, spec, jobName string, params JobParameters) error {
	if _, err := Parser.Parse(spec); err != nil {
		return fmt.Errorf("failed to parse cron expression %s: %w", spec, err)
	}

	s.updateMu.Lock()
	defer s.updateMu.Unlock()

	jobSchedule, err := s.jobScheduleRepository.Upsert(key, spec, jobName, params)
	if err != nil {
		return err
	}

	if s.running {
		s.scheduleJob(jobSchedule)
	}

	return nil
}

func (s *Dcron) Stop() context.Context {
	s.runningMu.Lock()
	defer s.runningMu.Unlock()

	s.running = false
	s.logger.Info("Stopping")

	return s.cron.Stop()
}
