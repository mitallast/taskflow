package org.github.mitallast.taskflow.aws;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.util.StringUtils;
import org.github.mitallast.taskflow.operation.OperationEnvironment;

import static com.amazonaws.SDKGlobalConfiguration.*;
import static com.amazonaws.SDKGlobalConfiguration.ALTERNATE_SECRET_KEY_ENV_VAR;
import static com.amazonaws.SDKGlobalConfiguration.SECRET_KEY_ENV_VAR;

public class OperationEnvironmentCredentialsProvider implements AWSCredentialsProvider {

    private final OperationEnvironment env;

    public OperationEnvironmentCredentialsProvider(OperationEnvironment env) {
        this.env = env;
    }

    @Override
    public AWSCredentials getCredentials() {
        String accessKey = env.get(ACCESS_KEY_ENV_VAR).orElse(null);
        if (accessKey == null) {
            accessKey = env.get(ALTERNATE_ACCESS_KEY_ENV_VAR).orElse(null);
        }

        String secretKey = env.get(SECRET_KEY_ENV_VAR).orElse(null);
        if (secretKey == null) {
            secretKey = env.get(ALTERNATE_SECRET_KEY_ENV_VAR).orElse(null);
        }

        accessKey = StringUtils.trim(accessKey);
        secretKey = StringUtils.trim(secretKey);
        String sessionToken =
            StringUtils.trim(env.get(AWS_SESSION_TOKEN_ENV_VAR).orElse(null));

        if (StringUtils.isNullOrEmpty(accessKey)
            || StringUtils.isNullOrEmpty(secretKey)) {

            throw new SdkClientException(
                "Unable to load AWS credentials from environment variables " +
                    "(" + ACCESS_KEY_ENV_VAR + " (or " + ALTERNATE_ACCESS_KEY_ENV_VAR + ") and " +
                    SECRET_KEY_ENV_VAR + " (or " + ALTERNATE_SECRET_KEY_ENV_VAR + "))");
        }

        return sessionToken == null ?
            new BasicAWSCredentials(accessKey, secretKey)
            :
            new BasicSessionCredentials(accessKey, secretKey, sessionToken);
    }

    @Override
    public void refresh() {
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
