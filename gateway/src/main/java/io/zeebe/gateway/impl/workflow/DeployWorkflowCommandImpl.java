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
package io.zeebe.gateway.impl.workflow;

import static io.zeebe.protocol.Protocol.DEPLOYMENT_PARTITION;
import static io.zeebe.util.EnsureUtil.ensureNotNull;

import io.zeebe.gateway.api.commands.DeployWorkflowCommandStep1;
import io.zeebe.gateway.api.commands.DeployWorkflowCommandStep1.DeployWorkflowCommandBuilderStep2;
import io.zeebe.gateway.api.commands.DeploymentResource;
import io.zeebe.gateway.api.commands.ResourceType;
import io.zeebe.gateway.api.events.DeploymentEvent;
import io.zeebe.gateway.cmd.ClientException;
import io.zeebe.gateway.impl.CommandImpl;
import io.zeebe.gateway.impl.RequestManager;
import io.zeebe.gateway.impl.command.DeploymentCommandImpl;
import io.zeebe.gateway.impl.command.DeploymentResourceImpl;
import io.zeebe.gateway.impl.record.RecordImpl;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.intent.DeploymentIntent;
import io.zeebe.util.StreamUtil;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class DeployWorkflowCommandImpl extends CommandImpl<DeploymentEvent>
    implements DeployWorkflowCommandStep1, DeployWorkflowCommandBuilderStep2 {
  private final DeploymentCommandImpl command = new DeploymentCommandImpl(DeploymentIntent.CREATE);

  private final List<DeploymentResource> resources = new ArrayList<>();

  public DeployWorkflowCommandImpl(final RequestManager commandManager) {
    super(commandManager);

    // send command always to the deployment partition
    this.command.setPartitionId(DEPLOYMENT_PARTITION);
  }

  @Override
  public DeployWorkflowCommandBuilderStep2 addResourceBytes(
      final byte[] resource, final String resourceName) {
    final DeploymentResourceImpl deploymentResource = new DeploymentResourceImpl();

    deploymentResource.setResource(resource);
    deploymentResource.setResourceName(resourceName);
    deploymentResource.setResourceType(getResourceType(resourceName));

    resources.add(deploymentResource);

    return this;
  }

  @Override
  public DeployWorkflowCommandBuilderStep2 addResourceString(
      final String resource, final Charset charset, final String resourceName) {
    return addResourceBytes(resource.getBytes(charset), resourceName);
  }

  @Override
  public DeployWorkflowCommandBuilderStep2 addResourceStringUtf8(
      final String resourceString, final String resourceName) {
    return addResourceString(resourceString, StandardCharsets.UTF_8, resourceName);
  }

  @Override
  public DeployWorkflowCommandBuilderStep2 addResourceStream(
      final InputStream resourceStream, final String resourceName) {
    ensureNotNull("resource stream", resourceStream);

    try {
      final byte[] bytes = StreamUtil.read(resourceStream);

      return addResourceBytes(bytes, resourceName);
    } catch (final IOException e) {
      final String exceptionMsg =
          String.format("Cannot deploy bpmn resource from stream. %s", e.getMessage());
      throw new ClientException(exceptionMsg, e);
    }
  }

  @Override
  public DeployWorkflowCommandBuilderStep2 addResourceFromClasspath(
      final String classpathResource) {
    ensureNotNull("classpath resource", classpathResource);

    try (final InputStream resourceStream =
        getClass().getClassLoader().getResourceAsStream(classpathResource)) {
      if (resourceStream != null) {
        return addResourceStream(resourceStream, classpathResource);
      } else {
        throw new FileNotFoundException(classpathResource);
      }

    } catch (final IOException e) {
      final String exceptionMsg =
          String.format("Cannot deploy resource from classpath. %s", e.getMessage());
      throw new RuntimeException(exceptionMsg, e);
    }
  }

  @Override
  public DeployWorkflowCommandBuilderStep2 addResourceFile(final String filename) {
    ensureNotNull("filename", filename);

    try (final InputStream resourceStream = new FileInputStream(filename)) {
      return addResourceStream(resourceStream, filename);
    } catch (final IOException e) {
      final String exceptionMsg =
          String.format("Cannot deploy resource from file. %s", e.getMessage());
      throw new RuntimeException(exceptionMsg, e);
    }
  }

  @Override
  public DeployWorkflowCommandBuilderStep2 addWorkflowModel(
      final BpmnModelInstance workflowDefinition, final String resourceName) {
    ensureNotNull("workflow model", workflowDefinition);

    final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    Bpmn.writeModelToStream(outStream, workflowDefinition);
    return addResourceBytes(outStream.toByteArray(), resourceName);
  }

  @Override
  public RecordImpl getCommand() {
    command.setResources(resources);
    return command;
  }

  private ResourceType getResourceType(String resourceName) {
    resourceName = resourceName.toLowerCase();

    if (resourceName.endsWith(".yaml")) {
      return ResourceType.YAML_WORKFLOW;
    } else if (resourceName.endsWith(".bpmn") || resourceName.endsWith(".bpmn20.xml")) {
      return ResourceType.BPMN_XML;
    } else {
      throw new RuntimeException(
          String.format("Cannot resolve type of resource '%s'.", resourceName));
    }
  }
}
