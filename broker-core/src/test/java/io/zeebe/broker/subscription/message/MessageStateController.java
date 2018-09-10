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
package io.zeebe.broker.subscription.message;

import io.zeebe.logstreams.state.StateController;
import java.io.File;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

public class MessageStateController extends StateController {
  private static final byte[] EXISTANCE = new byte[] {1};

  private static final byte[] MSG_TIME_TO_LIVE_NAME = "msgTimeToLive".getBytes();
  private static final byte[] MSG_ID_NAME = "messageId".getBytes();
  private static final byte[] SUB_NAME = "msgSubscription".getBytes();
  private static final byte[] SUB_SEND_TIME_NAME = "subSendTime".getBytes();

  private final UnsafeBuffer longBuffer = new UnsafeBuffer(0, 0);
  private final UnsafeBuffer iterateKeyBuffer = new UnsafeBuffer(0, 0);

  private ColumnFamilyHandle timeToLiveHandle;
  private ColumnFamilyHandle messageIdHandle;

  private ColumnFamilyHandle subscriptionHandle;
  private ColumnFamilyHandle subSendTimeHandle;

  private ExpandableArrayBuffer keyBuffer;
  private ExpandableArrayBuffer valueBuffer;

  @Override
  public RocksDB open(final File dbDirectory, final boolean reopen) throws Exception {
    final RocksDB rocksDB = super.open(dbDirectory, reopen);
    keyBuffer = new ExpandableArrayBuffer();
    valueBuffer = new ExpandableArrayBuffer();

    timeToLiveHandle =
        rocksDB.createColumnFamily(new ColumnFamilyDescriptor(MSG_TIME_TO_LIVE_NAME));
    messageIdHandle = rocksDB.createColumnFamily(new ColumnFamilyDescriptor(MSG_ID_NAME));
    subscriptionHandle = rocksDB.createColumnFamily(new ColumnFamilyDescriptor(SUB_NAME));
    subSendTimeHandle = rocksDB.createColumnFamily(new ColumnFamilyDescriptor(SUB_SEND_TIME_NAME));

    return rocksDB;
  }

  private int wrapKey(final DirectBuffer messageName, final DirectBuffer correlationKey) {
    int offset = 0;
    final int nameLength = messageName.capacity();
    keyBuffer.putBytes(0, messageName, 0, nameLength);
    offset += nameLength;

    final int correlationKeyLength = correlationKey.capacity();
    keyBuffer.putBytes(offset, correlationKey, 0, correlationKeyLength);
    offset += correlationKeyLength;
    return offset;
  }

  public void put(final Message message) {
    int offset = wrapKey(message.getName(), message.getCorrelationKey());

    message.write(valueBuffer, 0);

    put(keyBuffer.byteArray(), 0, offset, valueBuffer.byteArray(), 0, message.getLength());
    put(timeToLiveHandle, message.getDeadline(), keyBuffer.byteArray(), 0, offset);

    final DirectBuffer messageId = message.getId();
    final int messageIdLength = messageId.capacity();
    if (messageIdLength > 0) {
      keyBuffer.putBytes(offset, messageId, 0, messageIdLength);
      offset += messageIdLength;

      put(messageIdHandle, keyBuffer.byteArray(), 0, offset, EXISTANCE, 0, EXISTANCE.length);
    }
  }

  public void put(final MessageSubscription subscription) {
    final int offset = wrapKey(subscription.getMessageName(), subscription.getCorrelationKey());
    subscription.write(valueBuffer, 0);

    final int subscriptionLength = subscription.getLength();
    put(
        subscriptionHandle,
        keyBuffer.byteArray(),
        0,
        offset,
        valueBuffer.byteArray(),
        0,
        subscriptionLength);

    put(subSendTimeHandle, subscription.getCommandSentTime(), keyBuffer.byteArray(), 0, offset);
  }

  public Message findMessage(final DirectBuffer name, final DirectBuffer correlationKey) {
    final int length = wrapKey(name, correlationKey);
    return getMessage(keyBuffer, 0, length);
  }

  private Message getMessage(final DirectBuffer buffer, final int offset, final int length) {
    final int valueBufferSize = valueBuffer.capacity();
    final int readBytes =
        get(buffer.byteArray(), offset, length, valueBuffer.byteArray(), 0, valueBufferSize);

    if (readBytes > valueBufferSize) {
      throw new IllegalStateException("Not enough space in value buffer");
    }

    final Message message = new Message();
    message.wrap(valueBuffer, 0, readBytes);

    return message;
  }

  public MessageSubscription findSubscription(
      final DirectBuffer messageName, final DirectBuffer correlationKey) {

    final int length = wrapKey(messageName, correlationKey);
    return getSubscription(keyBuffer, 0, length);
  }

  private MessageSubscription getSubscription(
      final DirectBuffer buffer, final int offset, final int length) {
    final int valueBufferSize = valueBuffer.capacity();
    final int readBytes =
        get(
            subscriptionHandle,
            buffer.byteArray(),
            offset,
            length,
            valueBuffer.byteArray(),
            0,
            valueBufferSize);

    if (readBytes > valueBufferSize) {
      throw new IllegalStateException("Not enough space in value buffer");
    }

    final MessageSubscription subscription = new MessageSubscription();
    subscription.wrap(valueBuffer, 0, readBytes);

    return subscription;
  }

  public List<Message> findMessageBefore(final long timestamp) {
    final List<Message> messageList = new ArrayList<>();
    foreach(
        timeToLiveHandle,
        (key, value) -> {
          longBuffer.wrap(key);
          final long time = longBuffer.getLong(0, ByteOrder.LITTLE_ENDIAN);

          if (time <= timestamp) {
            iterateKeyBuffer.wrap(value);
            messageList.add(getMessage(iterateKeyBuffer, 0, value.length));
          }
        });
    return messageList;
  }

  public List<MessageSubscription> findSubscriptionBefore(final long deadline) {
    final List<MessageSubscription> subscriptionsList = new ArrayList<>();
    foreach(
        subSendTimeHandle,
        (key, value) -> {
          longBuffer.wrap(key);
          final long time = longBuffer.getLong(0, ByteOrder.LITTLE_ENDIAN);

          if (time > 0 && time < deadline) {
            iterateKeyBuffer.wrap(value);
            subscriptionsList.add(getSubscription(iterateKeyBuffer, 0, value.length));
          }
        });
    return subscriptionsList;
  }

  public boolean exist(final Message message) {
    int offset = wrapKey(message.getName(), message.getCorrelationKey());
    final int idLength = message.getId().capacity();
    keyBuffer.putBytes(offset, message.getId(), 0, idLength);
    offset += idLength;

    return exist(messageIdHandle, keyBuffer.byteArray(), 0, offset);
  }
}
