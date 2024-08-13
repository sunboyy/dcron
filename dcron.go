package dcron

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

const pollingInterval = time.Minute

var Parser = cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

type JobBuilder func(params JobParameters) cron.Job

type Dcron struct {
	jobBuilders           map[string]JobBuilder
	jobScheduleRepository JobScheduleRepository
	cron                  *cron.Cron
	mu                    sync.Mutex
	running               bool
	scheduleEntryID       map[uint]cron.EntryID
}

func New(jobScheduleRepository JobScheduleRepository) *Dcron {
	dcron := &Dcron{
		jobBuilders:           make(map[string]JobBuilder),
		jobScheduleRepository: jobScheduleRepository,
		cron:                  cron.New(),
		scheduleEntryID:       make(map[uint]cron.EntryID),
	}

	dcron.cron.Schedule(cron.Every(pollingInterval), cron.FuncJob(dcron.pollingUpdate))
	return dcron
}

func (s *Dcron) RegisterJobBuilder(name string, builder JobBuilder) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobBuilders[name] = builder
}

func (s *Dcron) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return
	}

	s.pollingUpdate()
	s.cron.Start()
	s.running = true

	log.Println("Dcron started")
}

func (s *Dcron) pollingUpdate() {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Println("Polling for updates")
	newJobSchedules, err := s.jobScheduleRepository.FindAll()
	if err != nil {
		log.Println("Failed to fetch job schedules:", err)
		return
	}

	// Create a set of new job schedule IDs for quick lookup
	newJobScheduleIDSet := make(map[uint]struct{}, len(newJobSchedules))
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

func (s *Dcron) scheduleJob(jobSchedule JobSchedule) error {
	if _, exists := s.scheduleEntryID[jobSchedule.ID]; exists {
		return nil // Already scheduled
	}

	schedule, err := Parser.Parse(jobSchedule.CronExpression)
	if err != nil {
		return fmt.Errorf("failed to parse cron expression %s: %w",
			jobSchedule.CronExpression, err)
	}

	job, ok := s.buildJob(jobSchedule.JobName, jobSchedule.Params)
	if !ok {
		return fmt.Errorf("failed to build job %s", jobSchedule.JobName)
	}

	entryID := s.cron.Schedule(schedule, newDistributedJob(s.jobScheduleRepository, jobSchedule, job))
	s.scheduleEntryID[jobSchedule.ID] = entryID

	log.Printf("Scheduled job %s with cron expression %s", jobSchedule.Key, jobSchedule.CronExpression)
	return nil
}

func (s *Dcron) buildJob(name string, params JobParameters) (cron.Job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	builder, exists := s.jobBuilders[name]
	if !exists {
		log.Printf("No job builder found for job name %s", name)
		return nil, false
	}

	return builder(params), true
}

func (s *Dcron) unschedule(id uint) {
	if entryID, exists := s.scheduleEntryID[id]; exists {
		s.cron.Remove(entryID)
		delete(s.scheduleEntryID, id)
		log.Printf("Unscheduled job with ID %d", id)
	}
}

func (s *Dcron) Schedule(key, spec, jobName string, params JobParameters) error {
	if _, err := Parser.Parse(spec); err != nil {
		return fmt.Errorf("failed to parse cron expression %s: %w", spec, err)
	}

	jobSchedule, err := s.jobScheduleRepository.Insert(key, spec, jobName, params)
	if err != nil {
		return err
	}

	if s.running {
		return s.scheduleJob(jobSchedule)
	}

	return nil
}

func (s *Dcron) Unschedule(key string) error {
	jobSchedule, err := s.jobScheduleRepository.FindByKey(key)
	if err != nil {
		return err
	}

	s.unschedule(jobSchedule.ID)
	return s.jobScheduleRepository.DeleteByKey(key)
}

func (s *Dcron) Stop() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.running = false
	log.Println("Dcron stopped")
	return s.cron.Stop()
}
