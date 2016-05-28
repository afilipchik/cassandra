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

package org.apache.cassandra.events.firehose;

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import org.apache.cassandra.events.MessageChannel;

/**
 * @author Alexander Filipchik (alexander.filipchik@am.sony.com)
 */
public class KinesisFirehoseMessageChannel implements MessageChannel
{
    private final AmazonKinesisFirehoseClient kinesisClient;
    private final AtomicLong counter = new AtomicLong();
    private final ExecutorService executor;
    private final String streamName;

    public KinesisFirehoseMessageChannel()
    {
        java.util.logging.Logger.getLogger("com").setLevel(java.util.logging.Level.OFF);
        ClientConfiguration configuration = new ClientConfiguration();
        configuration.setMaxConnections(1000);
        configuration.useTcpKeepAlive();
        executor = new ThreadPoolExecutor(1000, 1000, 10, TimeUnit.SECONDS,
                                          new LinkedBlockingQueue());

        kinesisClient = new AmazonKinesisFirehoseClient(KinesisFirehoseHelper.getCredentials(), configuration);
        kinesisClient.setEndpoint("firehose.eu-west-1.amazonaws.com");

        streamName = KinesisFirehoseHelper.createStream(kinesisClient);
    }

    public void sendMessage(String message)
    {
        executor.submit(new Sender(counter.getAndIncrement(), 0, message));
    }

    public class Sender implements Callable
    {

        long id;
        int runId;
        String message;

        public Sender(long id, int runId, String message)
        {
            this.id = id;
            this.runId = runId;
            this.message = message;
        }

        @Override
        public Object call() throws Exception
        {
            PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setDeliveryStreamName(streamName);

            putRecordRequest.setRecord(createRecord(id, runId, message));
            PutRecordResult recordResult = kinesisClient.putRecord(putRecordRequest);
            return recordResult;
        }

        private Record createRecord(long id, int runId, String message)
        {
            Record record = new Record();
            record.setData(ByteBuffer.wrap((message + '\n').getBytes()));
            return record;
        }
    }
}
