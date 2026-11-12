// Package repository is the data-access layer: S3 (job inputs), DynamoDB (progress, timeline)
// and the on-prem management cluster (JobSets, Kueue Workloads, Events).
//
// On the management cluster MultiKueue owns the JobSet (spec.managedBy) and mirrors the worker's JobSet status back,
// so JobSet status reflects the real training run. The Kueue Workload tells whether a worker
// admitted it and which one (status.clusterName).
package repository
