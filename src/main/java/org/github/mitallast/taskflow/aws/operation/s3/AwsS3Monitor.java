package org.github.mitallast.taskflow.aws.operation.s3;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigList;
import org.github.mitallast.taskflow.aws.AwsService;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.operation.Operation;
import org.github.mitallast.taskflow.operation.OperationCommand;
import org.github.mitallast.taskflow.operation.OperationResult;
import org.github.mitallast.taskflow.operation.OperationStatus;

import java.io.IOException;
import java.util.List;

public class AwsS3Monitor extends AbstractComponent implements Operation {

    private final AwsService awsService;

    @Inject
    public AwsS3Monitor(Config config, AwsService awsService) {
        super(config.getConfig("operation.aws.s3.monitor"), AwsS3Monitor.class);
        this.awsService = awsService;
    }

    @Override
    public String id() {
        return "aws-s3-monitor";
    }

    @Override
    public Config reference() {
        return config.getConfig("reference");
    }

    @Override
    public ConfigList schema() {
        return config.getList("schema");
    }

    @Override
    public OperationResult run(OperationCommand command) throws IOException, InterruptedException {
        Config config = command.config().withFallback(reference());

        AWSCredentialsProvider credentialsProvider = awsService.credentialsProvider(command.environment());
        AmazonS3 client = AmazonS3ClientBuilder.standard()
            .withCredentials(credentialsProvider)
            .withRegion(config.getString("region"))
            .build();

        String bucket = config.getString("bucket");
        String prefix = config.getString("prefix");
        long duration = config.getDuration("await").toMillis();
        long start = System.currentTimeMillis();
        long timeout = start + duration;

        while (!Thread.interrupted() && System.currentTimeMillis() < timeout) {
            logger.info("check exists s3://{}/{}", bucket, prefix);

            if (client.doesBucketExist(bucket)) {
                if (client.doesObjectExist(bucket, prefix)) {
                    return new OperationResult(OperationStatus.SUCCESS, "");
                }
            }
            logger.info("sleep 1s");
            Thread.sleep(1000);
        }

        if (Thread.interrupted()) {
            Thread.currentThread().interrupt();
            return new OperationResult(OperationStatus.FAILED, "Operation canceled");
        }

        return new OperationResult(OperationStatus.FAILED, "Operation timed out");
    }
}
