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
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import org.apache.cassandra.events.MessageChannel;

/**
 * @author Alexander Filipchik (alexander.filipchik@am.sony.com)
 */
public class KinesisFirehoseBatchMessageChannel implements MessageChannel
{
    private static final Logger logger = LoggerFactory.getLogger(KinesisFirehoseBatchMessageChannel.class);

    private AmazonKinesisFirehoseClient kinesisClient;
    private final AtomicLong counter = new AtomicLong();
    private final ExecutorService executor;
    private String streamName;
    private final BlockingQueue<Record> queue;

    public KinesisFirehoseBatchMessageChannel()
    {
        java.util.logging.Logger.getLogger("com").setLevel(java.util.logging.Level.OFF);
        ClientConfiguration configuration = new ClientConfiguration();
        configuration.setMaxConnections(100);
        configuration.useTcpKeepAlive();
        executor = new ThreadPoolExecutor(100, 100, 10, TimeUnit.SECONDS,
                                          new LinkedBlockingQueue());
        queue = new LinkedTransferQueue<>();
        for (int i = 0; i < 50; i++)
        {
            executor.submit(new BatchSender(queue, counter));
        }

        try
        {
            kinesisClient = new AmazonKinesisFirehoseClient(KinesisFirehoseHelper.getCredentials(), configuration);
            kinesisClient.setEndpoint("firehose.eu-west-1.amazonaws.com");

            streamName = KinesisFirehoseHelper.createStream(kinesisClient);
        }
        catch (Exception e)
        {
            logger.error(e.getMessage());
            kinesisClient = null;
            streamName = null;
        }
    }


    public void sendMessage(String message)
    {
        try
        {
            queue.offer(createRecord(counter.getAndIncrement(), 0, message));
        }
        catch (Exception e)
        {
            logger.error(e.getMessage());
        }
    }

    public class BatchSender implements Runnable
    {

        AtomicLong counterSent;
        BlockingQueue<Record> queue;
        boolean running = true;

        public BatchSender(final BlockingQueue<Record> queue, final AtomicLong counterSent)
        {
            this.queue = queue;
            this.counterSent = counterSent;
        }

        @Override
        public void run()
        {
            Collection<Record> records = new ArrayList<>(500);
            long lastTimeSent = System.currentTimeMillis();

            while (running)
            {
                try
                {
                    Record next = queue.poll(1, TimeUnit.SECONDS);
                    if (next != null)
                    {
                        records.add(next);
                    }

                    if (records.size() == 500 || System.currentTimeMillis() - lastTimeSent > 10000)
                    {
                        sendBatch(records);
                        records.clear();
                        lastTimeSent = System.currentTimeMillis();
                    }
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                    running = false;
                    Thread.interrupted();
                }
            }
        }

        private void sendBatch(final Collection<Record> records)
        {
            if (records.size() == 0)
            {
                return;
            }

            PutRecordBatchRequest putRecordRequest = new PutRecordBatchRequest();
            putRecordRequest.setDeliveryStreamName(streamName);

            putRecordRequest.setRecords(records);
            PutRecordBatchResult recordResult = kinesisClient.putRecordBatch(putRecordRequest);
            counterSent.addAndGet(records.size());
        }
    }

    private Record createRecord(long id, int runId, String message)
    {
        Record record = new Record();
        record.setData(ByteBuffer.wrap((id
                                        + ':' + System.currentTimeMillis()
                                        + ':' + message + '\n')
                                       .getBytes()));
        return record;
    }
}
