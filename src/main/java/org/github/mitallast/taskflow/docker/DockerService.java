package org.github.mitallast.taskflow.docker;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import org.github.mitallast.taskflow.common.component.AbstractComponent;

import java.util.Map;
import java.util.Properties;

import static com.github.dockerjava.core.DefaultDockerClientConfig.*;

public class DockerService extends AbstractComponent {

    private final DockerClientConfig dockerConfig;
    private final DockerClient docker;

    @Inject
    public DockerService(Config config) {
        super(config.getConfig("docker"), DockerService.class);

        DefaultDockerClientConfig.Builder builder = DefaultDockerClientConfig.createDefaultConfigBuilder();

        if (config.hasPath(DOCKER_HOST)) {
            builder.withDockerHost(this.config.getString(DOCKER_HOST));
        }
        if (config.hasPath(DOCKER_TLS_VERIFY)) {
            builder.withDockerTlsVerify(this.config.getString(DOCKER_TLS_VERIFY));
        }
        if (config.hasPath(DOCKER_CONFIG)) {
            builder.withDockerConfig(this.config.getString(DOCKER_CONFIG));
        }
        if (config.hasPath(DOCKER_CERT_PATH)) {
            builder.withDockerCertPath(this.config.getString(DOCKER_CERT_PATH));
        }
        if (config.hasPath(API_VERSION)) {
            builder.withApiVersion(this.config.getString(API_VERSION));
        }
        if (config.hasPath(REGISTRY_USERNAME)) {
            builder.withRegistryUsername(this.config.getString(REGISTRY_USERNAME));
        }
        if (config.hasPath(REGISTRY_PASSWORD)) {
            builder.withRegistryPassword(this.config.getString(REGISTRY_PASSWORD));
        }
        if (config.hasPath(REGISTRY_EMAIL)) {
            builder.withRegistryEmail(this.config.getString(REGISTRY_EMAIL));
        }
        if (config.hasPath(REGISTRY_URL)) {
            builder.withRegistryUrl(this.config.getString(REGISTRY_URL));
        }

        dockerConfig = builder.build();

        docker = DockerClientBuilder.getInstance(dockerConfig).build();
    }

    public DockerClient docker() {
        return docker;
    }
}
