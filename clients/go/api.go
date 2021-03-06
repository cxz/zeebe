package zbc

import (
	"github.com/zeebe-io/zeebe/clients/go/commands"
)

type ZBClient interface {
	NewTopologyCommand() *commands.TopologyCommand
	NewDeployWorkflowCommand() *commands.DeployCommand

	NewCreateInstanceCommand() commands.CreateInstanceCommandStep1
	NewCancelInstanceCommand() commands.CancelInstanceStep1
	NewUpdatePayloadCommand() commands.UpdatePayloadCommandStep1

	NewPublishMessageCommand() commands.PublishMessageCommandStep1

	NewCreateJobCommand() commands.CreateJobCommandStep1
	NewActivateJobsCommand() commands.ActivateJobsCommandStep1
	NewCompleteJobCommand() commands.CompleteJobCommandStep1
	NewFailJobCommand() commands.FailJobCommandStep1
	NewUpdateJobRetriesCommand() commands.UpdateJobRetriesCommandStep1

	Close() error
}
