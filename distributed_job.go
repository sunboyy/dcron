package dcron

import (
	"context"
	"log"
	"time"

	"github.com/robfig/cron/v3"
)

const lockTimeout = time.Minute * 5

// distributedJob is a wrapper around a cron.Job that manages the locking
// mechanism to allow distributed execution.
type distributedJob struct {
	jobScheduleRepository JobScheduleRepository
	jobSchedule           JobSchedule
	instanceID            string
	job                   cron.Job
}

func newDistributedJob(repo JobScheduleRepository, jobSchedule JobSchedule,
	instanceID string, job cron.Job) *distributedJob {

	return &distributedJob{
		jobScheduleRepository: repo,
		jobSchedule:           jobSchedule,
		instanceID:            instanceID,
		job:                   job,
	}
}

// Run is the method that is called by the cron scheduler to execute the job.
// It tries to acquire a lock on the job schedule before executing the job and
// releases the lock after the job is finished. If the lock cannot be acquired,
// the job is skipped.
func (j *distributedJob) Run() {
	if !j.acquireLock() {
		return
	}
	defer j.releaseLock()

	j.job.Run()
}

func (j *distributedJob) acquireLock() bool {
	ctx, cancel := context.WithTimeout(context.Background(), lockTimeout)
	defer cancel()

	locked, err := j.jobScheduleRepository.AcquireLock(ctx, j.instanceID,
		j.jobSchedule.ID, lockTimeout)
	if err != nil {
		log.Println("Error acquiring lock:", err)
	}
	return locked
}

func (j *distributedJob) releaseLock() {
	schedule, _ := Parser.Parse(j.jobSchedule.CronExpression)

	ctx, cancel := context.WithTimeout(context.Background(), lockTimeout)
	defer cancel()

	if err := j.jobScheduleRepository.ReleaseLock(ctx, j.instanceID,
		j.jobSchedule.ID, schedule.Next(time.Now())); err != nil {

		log.Println("Error releasing lock:", err)
	}
}
