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
package io.zeebe.broker.logstreams.processor;

import io.zeebe.broker.util.KeyStateController;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.msgpack.property.LongProperty;

public class KeyGenerator extends UnpackedObject {

  /*
   * Making sure these entities get unique keys on the same partition
   */
  private static final int STEP_SIZE = 5;
  private static final int WF_OFFSET = 1;
  private static final int JOB_OFFSET = 2;
  private static final int INCIDENT_OFFSET = 3;
  private static final int MESSAGE_OFFSET = 4;
  private static final int TOPIC_OFFSET = 5;

  private final LongProperty nextKey;
  private final int stepSize;
  private final KeyStateController keyStateController;

  public KeyGenerator(long initialValue, int stepSize) {
    this(initialValue, stepSize, null);
  }

  public KeyGenerator(long initialValue, int stepSize, KeyStateController keyStateController) {
    nextKey = new LongProperty("nextKey", initialValue);
    this.stepSize = stepSize;
    declareProperty(nextKey);
    this.keyStateController = keyStateController;
    init(initialValue);
  }

  private void init(long initialValue) {
    if (keyStateController != null) {
      keyStateController.addOnOpenCallback(
          () -> {
            final long latestKey = keyStateController.getNextKey();
            if (latestKey > 0) {
              setKey(latestKey);
            } else {
              keyStateController.putNextKey(initialValue);
            }
          });
    }
  }

  public long nextKey() {
    final long key = nextKey.getValue();
    nextKey.setValue(key + stepSize);
    putLatestKeyIntoController(key + stepSize);
    return key;
  }

  public void setKey(long key) {
    final long nextKey = key + stepSize;
    this.nextKey.setValue(nextKey);
    putLatestKeyIntoController(nextKey);
  }

  private void putLatestKeyIntoController(long key) {
    if (keyStateController != null) {
      keyStateController.putNextKey(key);
    }
  }

  public static KeyGenerator createWorkflowInstanceKeyGenerator() {
    return new KeyGenerator(WF_OFFSET, STEP_SIZE);
  }

  public static KeyGenerator createJobKeyGenerator(KeyStateController keyStateController) {
    return new KeyGenerator(JOB_OFFSET, STEP_SIZE, keyStateController);
  }

  public static KeyGenerator createIncidentKeyGenerator() {
    return new KeyGenerator(INCIDENT_OFFSET, STEP_SIZE);
  }

  public static KeyGenerator createMessageKeyGenerator(KeyStateController keyStateController) {
    return new KeyGenerator(MESSAGE_OFFSET, STEP_SIZE, keyStateController);
  }

  public static KeyGenerator createTopicKeyGenerator() {
    return new KeyGenerator(TOPIC_OFFSET, STEP_SIZE);
  }
}
