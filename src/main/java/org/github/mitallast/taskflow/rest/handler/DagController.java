package org.github.mitallast.taskflow.rest.handler;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import org.github.mitallast.taskflow.dag.DagPersistenceService;
import org.github.mitallast.taskflow.rest.RestController;

public class DagController {

    @Inject
    public DagController(RestController controller, DagPersistenceService service) {

        controller.handler(service::findDagById)
            .param1(controller.param().toLong("id"))
            .response(controller.response().optionalJson())
            .handle(HttpMethod.GET, "/api/dag/id/{id}");

        controller.handler(service::findDagByToken)
            .param1(controller.param().string("token"))
            .response(controller.response().optionalJson())
            .handle(HttpMethod.GET, "/api/dag/token/{token}");
    }
}
