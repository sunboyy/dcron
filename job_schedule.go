package dcron

import (
	"context"
	"time"
)

type JobParameters map[string]interface{}

type JobSchedule struct {
	ID             int64
	Key            string
	JobName        string
	Params         JobParameters
	CronExpression string
}

// JobScheduleRepository is an interface for a repository that manages job
// schedules.
type JobScheduleRepository interface {
	// FindAll returns all job schedules.
	FindAll() ([]JobSchedule, error)

	// FindByKey finds the job schedule with the given key. It returns the job
	// schedule and an error if the job schedule is not found.
	FindByKey(key string) (*JobSchedule, error)

	// Insert inserts a new job schedule with the given key, cron expression,
	// job name, and parameters. It returns the inserted job schedule.
	Insert(key, spec, jobName string, params JobParameters) (JobSchedule, error)

	// Upsert inserts a new job schedule with the given key, cron expression,
	// job name, and parameters. If the job schedule already exists, it updates
	// the existing job schedule. It returns the inserted job schedule.
	Upsert(key, spec, jobName string, params JobParameters) (JobSchedule, error)

	// DeleteByKey deletes the job schedule with the given key.
	DeleteByKey(key string) error

	// AcquireLock tries to update the lock acquired time on the job schedule and
	// returns the result. There are cases where the lock cannot be acquired:
	//
	// 1. There is already a lock acquired by another process within the timeout.
	// 2. The job is already scheduled to run at a later time.
	// 3. The job schedule is deleted.
	// 4. An error occurs while updating the database.
	AcquireLock(ctx context.Context, instanceID string, scheduleID int64,
		timeout time.Duration) (bool, error)

	// ReleaseLock resets the lock acquired time of the job schedule to nil while
	// also updating the next run time to prevent the job from re-running.
	ReleaseLock(ctx context.Context, instanceID string, scheduleID int64,
		nextRunAt time.Time) error
}
