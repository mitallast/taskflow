package org.github.mitallast.taskflow.aws;

import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.operation.OperationEnvironment;

public class AwsService extends AbstractComponent {

    private final ConfigCredentialsProvider configProvider;
    private final EnvironmentVariableCredentialsProvider environmentProvider;
    private final SystemPropertiesCredentialsProvider systemPropertiesProvider;
    private final ProfileCredentialsProvider profileProvider;
    private final EC2ContainerCredentialsProviderWrapper ec2Provider;

    @Inject
    public AwsService(Config config) {
        super(config.getConfig("aws"), AwsService.class);

        configProvider = new ConfigCredentialsProvider(this.config);
        environmentProvider = new EnvironmentVariableCredentialsProvider();
        systemPropertiesProvider = new SystemPropertiesCredentialsProvider();
        profileProvider = new ProfileCredentialsProvider();
        ec2Provider = new EC2ContainerCredentialsProviderWrapper();
    }

    public AWSCredentialsProvider credentialsProvider(OperationEnvironment environment) {
        return new AWSCredentialsProviderChain(
            new OperationEnvironmentCredentialsProvider(environment),
            configProvider,
            environmentProvider,
            systemPropertiesProvider,
            profileProvider,
            ec2Provider
        );
    }
}
