/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.workflow.model.transformation.handler;

import io.zeebe.broker.workflow.model.element.ExecutableMessage;
import io.zeebe.broker.workflow.model.transformation.ModelElementTransformer;
import io.zeebe.broker.workflow.model.transformation.TransformContext;
import io.zeebe.model.bpmn.instance.Message;
import io.zeebe.model.bpmn.instance.zeebe.ZeebeSubscription;
import io.zeebe.msgpack.jsonpath.JsonPathQuery;
import io.zeebe.msgpack.jsonpath.JsonPathQueryCompiler;
import io.zeebe.util.buffer.BufferUtil;

public class MessageHandler implements ModelElementTransformer<Message> {

  @Override
  public Class<Message> getType() {
    return Message.class;
  }

  @Override
  public void transform(Message element, TransformContext context) {

    final String id = element.getId();
    final ExecutableMessage executableElement = new ExecutableMessage(id);

    final ZeebeSubscription subscription =
        element
            .getExtensionElements()
            .getElementsQuery()
            .filterByType(ZeebeSubscription.class)
            .singleResult();

    final JsonPathQueryCompiler queryCompiler = context.getJsonPathQueryCompiler();
    final JsonPathQuery query = queryCompiler.compile(subscription.getCorrelationKey());

    executableElement.setCorrelationKey(query);
    executableElement.setMessageName(BufferUtil.wrapString(element.getName()));

    context.addMessage(executableElement);
  }
}
