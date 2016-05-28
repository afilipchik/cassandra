/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.events;

import java.net.InetAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.events.firehose.KinesisFirehoseHelper;

/**
 * @author Alexander Filipchik (alexander.filipchik@am.sony.com)
 */
public class MessageReceivedEvent implements Event
{

    private static final Logger logger = LoggerFactory.getLogger(MessageReceivedEvent.class);
    // logger.error("Received from {}: {} KS {} CF {} K {} C {} Size {} Timestamp {} at {}. Diff is: {}",

    private InetAddress senderIp;
    private String keySpace;
    private String columnFamily;
    private String key;
    private String columnName;
    private short delete;
    private int dataSize;
    private long createdTimestamp;
    private long receivedTimestamp;

    public MessageReceivedEvent(InetAddress senderIp, String keySpace, String columnFamily, String key,
                                String columnName, short delete, int dataSize, long createdTimestamp,
                                long receivedTimestamp)
    {
        this.senderIp = senderIp;
        this.keySpace = keySpace;
        this.columnFamily = columnFamily;
        this.key = key;
        this.columnName = columnName;
        this.delete = delete;
        this.dataSize = dataSize;
        this.createdTimestamp = createdTimestamp;
        this.receivedTimestamp = receivedTimestamp;
    }

    public String getString()
    {

        return " From:" + ((senderIp == null) ? "local" : KinesisFirehoseHelper.getNodeDescription(senderIp, '_'))
               + ':' + keySpace + ':' + columnFamily + ':'
               + key + ':' + columnName + ':' + delete + ':' + dataSize + ':'
               + createdTimestamp + ':' + receivedTimestamp;
    }
}
