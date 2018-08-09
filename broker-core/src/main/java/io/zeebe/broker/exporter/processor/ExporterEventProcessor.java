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
package io.zeebe.broker.exporter.processor;

import io.zeebe.exporter.spi.Event;
import io.zeebe.exporter.spi.Exporter;
import io.zeebe.logstreams.log.LogStreamRecordWriter;
import io.zeebe.logstreams.processor.EventLifecycleContext;
import io.zeebe.logstreams.processor.EventProcessor;

public class ExporterEventProcessor implements EventProcessor {
  private final Exporter exporter;
  private Event currentEvent;

  public ExporterEventProcessor(final Exporter exporter) {
    this.exporter = exporter;
  }

  public void wrap(Event event) {
    this.currentEvent = event;
  }

  @Override
  public void processEvent(EventLifecycleContext ctx) {}

  @Override
  public boolean executeSideEffects() {
    if (currentEvent == null) {
      return false;
    }

    currentEvent = null;
    return true;
  }

  @Override
  public long writeEvent(LogStreamRecordWriter writer) {
    return 0;
  }

  @Override
  public void updateState() {}
}
