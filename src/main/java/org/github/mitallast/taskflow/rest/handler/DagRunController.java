package org.github.mitallast.taskflow.rest.handler;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.github.mitallast.taskflow.dag.DagPersistenceService;
import org.github.mitallast.taskflow.rest.RestController;

public class DagRunController {

    @Inject
    public DagRunController(RestController controller, DagPersistenceService persistenceService) {
        controller.handler(persistenceService::findDagRuns)
            .response(controller.response().json())
            .handle(HttpMethod.GET, "api/dag/run");

        controller.handler(persistenceService::findPendingDagRuns)
            .response(controller.response().json())
            .handle(HttpMethod.GET, "api/dag/run/pending");

        controller.handler(persistenceService::findDagRun)
            .param1(controller.param().toLong("id"))
            .response(controller.response().optionalJson())
            .handle(HttpMethod.GET, "api/dag/run/id/{id}");

        controller.handler(persistenceService::startDagRun)
            .param1(controller.param().toLong("id"))
            .response((request, result) -> {
                if (result) request.response().status(HttpResponseStatus.ACCEPTED).empty();
                else request.response().status(HttpResponseStatus.METHOD_NOT_ALLOWED).empty();
            })
            .handle(HttpMethod.POST, "api/dag/run/id/{id}/start");

        controller.handler(persistenceService::markDagRunCanceled)
            .param1(controller.param().toLong("id"))
            .response((request, result) -> {
                if (result) request.response().status(HttpResponseStatus.ACCEPTED).empty();
                else request.response().status(HttpResponseStatus.METHOD_NOT_ALLOWED).empty();
            })
            .handle(HttpMethod.POST, "api/dag/run/id/{id}/cancel");
    }
}
