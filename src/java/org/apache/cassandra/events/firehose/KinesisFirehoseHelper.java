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
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;

/**
 * @author Alexander Filipchik (alexander.filipchik@am.sony.com)
 */
public class KinesisFirehoseHelper
{

    public static AWSCredentials getCredentials() {
        String accessKey = DatabaseDescriptor.getKinesisFirehoseAwsAccessKey();
        String secretKey = DatabaseDescriptor.getKinesisFirehoseAwsSecretKey();
        return new BasicAWSCredentials(accessKey, secretKey);
    }

    public static String createStream(AmazonKinesisFirehose client) {
        String prefix;
        String deliveryStreamName;
        String localDc = DatabaseDescriptor.getLocalDataCenter();
        String localRack = DatabaseDescriptor.getEndpointSnitch().getRack(FBUtilities.getBroadcastAddress());
        String localIp = DatabaseDescriptor.getListenAddress().getHostAddress();
        deliveryStreamName = DatabaseDescriptor.getKinesisFirehoseStreamNamePrefix() +
                             localDc + '_' + localRack + '_' + localIp;
        prefix = "replication_lag/" + localDc + '/' + localRack + '/' + localIp;

        try
        {
            CreateDeliveryStreamRequest createDeliveryStreamRequest = new CreateDeliveryStreamRequest();
            createDeliveryStreamRequest.setDeliveryStreamName(deliveryStreamName);

            S3DestinationConfiguration s3DestinationConfiguration = new S3DestinationConfiguration();
            s3DestinationConfiguration.setBucketARN(DatabaseDescriptor.getKinesisFirehoseS3BucketArn());
            s3DestinationConfiguration.setPrefix(prefix);

            s3DestinationConfiguration.setCompressionFormat(CompressionFormat.UNCOMPRESSED);
            EncryptionConfiguration encryptionConfiguration = new EncryptionConfiguration();
            encryptionConfiguration.setNoEncryptionConfig(NoEncryptionConfig.NoEncryption);
            s3DestinationConfiguration.setEncryptionConfiguration(encryptionConfiguration);
            BufferingHints bufferingHints = new BufferingHints();
            bufferingHints.setIntervalInSeconds(60);
            bufferingHints.setSizeInMBs(128);
            s3DestinationConfiguration.setBufferingHints(bufferingHints);

            s3DestinationConfiguration.setRoleARN(DatabaseDescriptor.getKinesisFirehoseRoleArn());
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
