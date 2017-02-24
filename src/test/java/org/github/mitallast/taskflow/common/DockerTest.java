package org.github.mitallast.taskflow.common;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.SearchItem;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.command.PullImageResultCallback;
import org.junit.Test;

import java.util.List;

public class DockerTest {
    @Test
    public void test() throws Exception {
        DockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder()
//            .withDockerHost("tcp://localhost:2376")
//            .withDockerTlsVerify(true)
//            .withDockerCertPath("/home/user/.docker/certs")
//            .withDockerConfig("/home/user/.docker")
//            .withApiVersion("1.23")
//            .withRegistryUrl("https://index.docker.io/v1/")
//            .withRegistryUsername("dockeruser")
//            .withRegistryPassword("ilovedocker")
//            .withRegistryEmail("dockeruser@github.com")
            .build();
        DockerClient docker = DockerClientBuilder.getInstance(config).build();

        List<SearchItem> searchItems = docker.searchImagesCmd("hello-world").exec();
        System.out.println("Search returned: " + searchItems);

        String name = searchItems.get(0).getName();
//        docker.pullImageCmd(name).exec(new PullImageResultCallback()).awaitSuccess();

        CreateContainerResponse container = docker.createContainerCmd(name).exec();

        docker.startContainerCmd(container.getId()).exec();

        docker.stopContainerCmd(container.getId()).exec();

        docker.removeContainerCmd(container.getId()).exec();

        System.out.println();
    }
}
