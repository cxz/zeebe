/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.gateway.api.commands;

import io.zeebe.gateway.api.events.JobActivateEvent;

public interface ActivateJobsCommandStep1 extends FinalCommandStep<JobActivateEvent> {

  ActivateJobsCommandStep1 jobType(String jobType);

  ActivateJobsCommandStep1 amount(int amount);

  ActivateJobsCommandStep1 workerName(String workerName);

  ActivateJobsCommandStep1 timeout(long timeout);
}
