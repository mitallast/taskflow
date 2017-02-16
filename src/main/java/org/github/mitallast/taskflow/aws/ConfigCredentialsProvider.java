package org.github.mitallast.taskflow.aws;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.util.StringUtils;
import com.typesafe.config.Config;

public class ConfigCredentialsProvider implements AWSCredentialsProvider {

    public static final String ACCESS_KEY_ENV_VAR = "access_key_id";
    public static final String SECRET_KEY_ENV_VAR = "secret_key";
    public static final String AWS_SESSION_TOKEN_ENV_VAR = "session_token";

    private final Config config;

    public ConfigCredentialsProvider(Config config) {
        this.config = config;
    }

    @Override
    public AWSCredentials getCredentials() {
        String accessKey = config.getString(ACCESS_KEY_ENV_VAR);
        String secretKey = config.getString(SECRET_KEY_ENV_VAR);

        accessKey = StringUtils.trim(accessKey);
        secretKey = StringUtils.trim(secretKey);
        String sessionToken = StringUtils.trim(config.getString(AWS_SESSION_TOKEN_ENV_VAR));

        if (StringUtils.isNullOrEmpty(accessKey)
            || StringUtils.isNullOrEmpty(secretKey)) {

            throw new SdkClientException(
                "Unable to load AWS credentials from environment variables " +
                    "(" + ACCESS_KEY_ENV_VAR + " and " +
                    SECRET_KEY_ENV_VAR + ")");
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
