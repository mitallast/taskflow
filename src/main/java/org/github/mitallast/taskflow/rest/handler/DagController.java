package org.github.mitallast.taskflow.rest.handler;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.dag.Dag;
import org.github.mitallast.taskflow.dag.DagPersistenceService;
import org.github.mitallast.taskflow.rest.RestController;
import org.github.mitallast.taskflow.rest.RestRequest;

import java.util.Optional;

public class DagController extends AbstractComponent {

    private final DagPersistenceService service;

    @Inject
    public DagController(Config config, RestController controller, DagPersistenceService service) {
        super(config, DagController.class);
        this.service = service;

        controller.registerHandler(HttpMethod.GET, "/api/dag/token/{token}", this::findByToken);
    }

    public void findByToken(RestRequest request) {
        Optional<Dag> dag = service.findDag(request.param("token"));
        if (dag.isPresent()) {
            request.response().json(dag.get());
        } else {
            request.response().status(HttpResponseStatus.NOT_FOUND).empty();
        }
    }
}
