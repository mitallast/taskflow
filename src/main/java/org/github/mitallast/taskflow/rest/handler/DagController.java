package org.github.mitallast.taskflow.rest.handler;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.github.mitallast.taskflow.dag.Dag;
import org.github.mitallast.taskflow.dag.DagPersistenceService;
import org.github.mitallast.taskflow.dag.DagService;
import org.github.mitallast.taskflow.rest.RestController;

public class DagController {

    @Inject
    public DagController(RestController controller, DagService dagService, DagPersistenceService persistenceService) {

        /*
         * Dag API
         */

        controller.handler(dagService::validate)
            .param1(controller.param().json(Dag.class))
            .response((request, errors) -> {
                if (errors.valid()) {
                    request.response().status(HttpResponseStatus.NO_CONTENT).empty();
                } else {
                    request.response().status(HttpResponseStatus.NOT_ACCEPTABLE).json(errors);
                }
            })
            .handle(HttpMethod.PUT, "api/dag/validate");

        controller.handler(persistenceService::createDag)
            .param1(controller.param().json(Dag.class))
            .response(controller.response().json())
            .handle(HttpMethod.PUT, "api/dag");

        controller.handler(persistenceService::updateDag)
            .param1(controller.param().json(Dag.class))
            .response(controller.response().json())
            .handle(HttpMethod.POST, "api/dag");

        controller.handler(persistenceService::findLatestDags)
            .response(controller.response().json())
            .handle(HttpMethod.GET, "api/dag/latest");

        controller.handler(persistenceService::findDagById)
            .param1(controller.param().toLong("id"))
            .response(controller.response().optionalJson())
            .handle(HttpMethod.GET, "api/dag/id/{id}");

        controller.handler(persistenceService::findDagByToken)
            .param1(controller.param().string("token"))
            .response(controller.response().optionalJson())
            .handle(HttpMethod.GET, "api/dag/token/{token}");

        /*
         * DagRun API
         */

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
