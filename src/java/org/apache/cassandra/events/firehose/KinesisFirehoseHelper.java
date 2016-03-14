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

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.model.BufferingHints;
import com.amazonaws.services.kinesisfirehose.model.CompressionFormat;
import com.amazonaws.services.kinesisfirehose.model.CreateDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.EncryptionConfiguration;
import com.amazonaws.services.kinesisfirehose.model.NoEncryptionConfig;
import com.amazonaws.services.kinesisfirehose.model.ResourceInUseException;
import com.amazonaws.services.kinesisfirehose.model.S3DestinationConfiguration;

/**
 * @author Alexander Filipchik (alexander.filipchik@am.sony.com)
 */
public class KinesisFirehoseHelper
{

    public static AWSCredentials getCredentials() {
        String accessKey = "";
        String secretKey = "";
        return new BasicAWSCredentials(accessKey, secretKey);
    }

    public static String createStream(AmazonKinesisFirehose client) {
        String prefix;
        String deliveryStreamName;
        try
        {
            deliveryStreamName = "" +
                                 InetAddress.getLocalHost().getHostName();
            prefix = "replication_lag/" + 0 + "/" +
                     InetAddress.getLocalHost().getHostName();
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }

        try
        {
            CreateDeliveryStreamRequest createDeliveryStreamRequest = new CreateDeliveryStreamRequest();
            createDeliveryStreamRequest.setDeliveryStreamName(deliveryStreamName);

            S3DestinationConfiguration s3DestinationConfiguration = new S3DestinationConfiguration();
            s3DestinationConfiguration.setBucketARN("");
            s3DestinationConfiguration.setPrefix(prefix);

            s3DestinationConfiguration.setCompressionFormat(CompressionFormat.UNCOMPRESSED);
            EncryptionConfiguration encryptionConfiguration = new EncryptionConfiguration();
            encryptionConfiguration.setNoEncryptionConfig(NoEncryptionConfig.NoEncryption);
            s3DestinationConfiguration.setEncryptionConfiguration(encryptionConfiguration);
            BufferingHints bufferingHints = new BufferingHints();
            bufferingHints.setIntervalInSeconds(60);
            bufferingHints.setSizeInMBs(128);
            s3DestinationConfiguration.setBufferingHints(bufferingHints);

            s3DestinationConfiguration.setRoleARN("");
            createDeliveryStreamRequest.setS3DestinationConfiguration(s3DestinationConfiguration);

            client.createDeliveryStream(createDeliveryStreamRequest);
            return createDeliveryStreamRequest.getDeliveryStreamName();
        }
        catch (ResourceInUseException e)
        {
            return deliveryStreamName;
        }
    }
}
