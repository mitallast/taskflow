package org.github.mitallast.taskflow.rest.handler;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.github.mitallast.taskflow.dag.*;
import org.github.mitallast.taskflow.rest.RestController;

import java.util.Optional;

public class DagController {

    private final DagService dagService;
    private final DagPersistenceService persistenceService;

    @Inject
    public DagController(RestController controller, DagService dagService, DagPersistenceService persistenceService) {
        this.dagService = dagService;
        this.persistenceService = persistenceService;

        controller.handler(dagService::validate)
            .param(controller.param().json(Dag.class))
            .response((request, errors) -> {
                if (errors.valid()) {
                    request.response().status(HttpResponseStatus.NO_CONTENT).empty();
                } else {
                    request.response().status(HttpResponseStatus.NOT_ACCEPTABLE).json(errors);
                }
            })
            .handle(HttpMethod.PUT, "api/dag/validate");

        controller.handler(dagService::createDag)
            .param(controller.param().json(Dag.class))
            .response(controller.response().maybeJson())
            .handle(HttpMethod.PUT, "api/dag");

        controller.handler(dagService::updateDag)
            .param(controller.param().json(Dag.class))
            .response(controller.response().maybeJson())
            .handle(HttpMethod.POST, "api/dag");

        controller.handler(persistenceService::findLatestDags)
            .response(controller.response().json())
            .handle(HttpMethod.GET, "api/dag/latest");

        controller.handler(persistenceService::findDagById)
            .param(controller.param().toLong("id"))
            .response(controller.response().optionalJson())
            .handle(HttpMethod.GET, "api/dag/id/{id}");

        controller.handler(persistenceService::findDagByToken)
            .param(controller.param().string("token"))
            .response(controller.response().optionalJson())
            .handle(HttpMethod.GET, "api/dag/token/{token}");

        controller.handler(this::runDagById)
            .param(controller.param().toLong("id"))
            .response(controller.response().optionalJson())
            .handle(HttpMethod.PUT, "api/dag/id/{id}/run");

        controller.handler(this::runDagByToken)
            .param(controller.param().string("token"))
            .response(controller.response().optionalJson())
            .handle(HttpMethod.PUT, "api/dag/token/{token}/run");
    }

    private Optional<DagRun> runDagById(long id) {
        return persistenceService.findDagById(id).map(dagService::createDagRun);
    }

    private Optional<DagRun> runDagByToken(String token) {
        return persistenceService.findDagByToken(token).map(dagService::createDagRun);
    }
}
