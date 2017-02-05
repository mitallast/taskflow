package org.github.mitallast.taskflow.rest.handler;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.github.mitallast.taskflow.dag.Dag;
import org.github.mitallast.taskflow.dag.DagPersistenceService;
import org.github.mitallast.taskflow.rest.RestController;

public class DagController {

    @Inject
    public DagController(RestController controller, DagPersistenceService service) {

        /*
         * Dag API
         */

        controller.handler(service::createDag)
            .param1(controller.param().json(Dag.class))
            .response(controller.response().json())
            .handle(HttpMethod.PUT, "api/dag");

        controller.handler(service::updateDag)
            .param1(controller.param().json(Dag.class))
            .response(controller.response().json())
            .handle(HttpMethod.POST, "api/dag");

        controller.handler(service::findLatestDags)
            .response(controller.response().json())
            .handle(HttpMethod.GET, "api/dag/latest");

        controller.handler(service::findDagById)
            .param1(controller.param().toLong("id"))
            .response(controller.response().optionalJson())
            .handle(HttpMethod.GET, "api/dag/id/{id}");

        controller.handler(service::findDagByToken)
            .param1(controller.param().string("token"))
            .response(controller.response().optionalJson())
            .handle(HttpMethod.GET, "api/dag/token/{token}");

        /*
         * DagRun API
         */

        controller.handler(service::findDagRuns)
            .response(controller.response().json())
            .handle(HttpMethod.GET, "api/dag/run");

        controller.handler(service::findPendingDagRuns)
            .response(controller.response().json())
            .handle(HttpMethod.GET, "api/dag/run/pending");

        controller.handler(service::findDagRun)
            .param1(controller.param().toLong("id"))
            .response(controller.response().optionalJson())
            .handle(HttpMethod.GET, "api/dag/run/id/{id}");

        controller.handler(service::startDagRun)
            .param1(controller.param().toLong("id"))
            .response((request, result) -> {
                if (result) request.response().status(HttpResponseStatus.ACCEPTED).empty();
                else request.response().status(HttpResponseStatus.METHOD_NOT_ALLOWED).empty();
            })
            .handle(HttpMethod.POST, "api/dag/run/id/{id}/start");

        controller.handler(service::markDagRunCanceled)
            .param1(controller.param().toLong("id"))
            .response((request, result) -> {
                if (result) request.response().status(HttpResponseStatus.ACCEPTED).empty();
                else request.response().status(HttpResponseStatus.METHOD_NOT_ALLOWED).empty();
            })
            .handle(HttpMethod.POST, "api/dag/run/id/{id}/cancel");
    }
}
