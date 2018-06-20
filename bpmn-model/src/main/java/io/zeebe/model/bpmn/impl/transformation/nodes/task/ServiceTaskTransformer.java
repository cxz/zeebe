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
package io.zeebe.model.bpmn.impl.transformation.nodes.task;

import java.util.List;
import io.zeebe.model.bpmn.impl.error.ErrorCollector;
import io.zeebe.model.bpmn.impl.instance.ExtensionElementsImpl;
import io.zeebe.model.bpmn.impl.instance.ServiceTaskImpl;
import io.zeebe.model.bpmn.impl.metadata.TaskHeadersImpl;

public class ServiceTaskTransformer {
  private final TaskHeadersTransformer taskHeadersTransformer = new TaskHeadersTransformer();

  public void transform(ErrorCollector errorCollector, List<ServiceTaskImpl> serviceTasks) {
    for (int s = 0; s < serviceTasks.size(); s++) {
      final ServiceTaskImpl serviceTaskImpl = serviceTasks.get(s);

      transformTaskHeaders(serviceTaskImpl.getExtensionElements());
    }
  }

  private void transformTaskHeaders(ExtensionElementsImpl extensionElements) {
    TaskHeadersImpl taskHeaders = extensionElements.getTaskHeaders();
    if (taskHeaders == null) {
      taskHeaders = new TaskHeadersImpl();
      extensionElements.setTaskHeaders(taskHeaders);
    }
    taskHeadersTransformer.transform(taskHeaders);
  }

}
