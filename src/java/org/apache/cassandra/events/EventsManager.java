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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.events.firehose.KinesisFirehoseBatchMessageChannel;
import org.apache.cassandra.events.firehose.KinesisFirehoseMessageChannel;

/**
 * @author Alexander Filipchik (alexander.filipchik@am.sony.com)
 */
public class EventsManager
{

    private static final Logger logger = LoggerFactory.getLogger(EventsManager.class);

    private static EventsManager instance;
    private MessageChannel channel;

    private EventsManager()
    {
//        channel = new KinesisFirehoseMessageChannel();
        channel = new KinesisFirehoseBatchMessageChannel();
    }

    public static EventsManager getInstance() {
        // Not Thread safe
        if (instance == null) {
            instance = new EventsManager();
        }

        return instance;
    }

    public void sendEvent(MessageReceivedEvent event) {
        try
        {
            channel.sendMessage(event.getString());
        }
        catch (Exception e)
        {
            logger.error(e.getMessage());
        }
    }
}
