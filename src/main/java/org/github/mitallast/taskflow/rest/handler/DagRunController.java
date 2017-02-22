package org.github.mitallast.taskflow.rest.handler;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.github.mitallast.taskflow.dag.DagRunPersistenceService;
import org.github.mitallast.taskflow.rest.RestController;
import org.github.mitallast.taskflow.executor.DagRunExecutor;

public class DagRunController {

    private final DagRunExecutor dagRunExecutor;

    @Inject
    public DagRunController(
        RestController controller,
        DagRunPersistenceService persistenceService,
        DagRunExecutor dagRunExecutor
    ) {
        this.dagRunExecutor = dagRunExecutor;

        controller.handler(persistenceService::findDagRuns)
            .response(controller.response().json())
            .handle(HttpMethod.GET, "api/dag/run");

        controller.handler(persistenceService::findPendingDagRuns)
            .response(controller.response().json())
            .handle(HttpMethod.GET, "api/dag/run/pending");

        controller.handler(persistenceService::findDagRun)
            .param(controller.param().toLong("id"))
            .response(controller.response().optionalJson())
            .handle(HttpMethod.GET, "api/dag/run/id/{id}");

        controller.handler(persistenceService::startDagRun)
            .param(controller.param().toLong("id"))
            .response((request, result) -> {
                if (result) request.response().status(HttpResponseStatus.NO_CONTENT).empty();
                else request.response().status(HttpResponseStatus.METHOD_NOT_ALLOWED).empty();
            })
            .handle(HttpMethod.POST, "api/dag/run/id/{id}/start");

        controller.handler(this::cancel)
            .param(controller.param().toLong("id"))
            .response((request, result) -> {
                if (result) request.response().status(HttpResponseStatus.NO_CONTENT).empty();
                else request.response().status(HttpResponseStatus.METHOD_NOT_ALLOWED).empty();
            })
            .handle(HttpMethod.POST, "api/dag/run/id/{id}/cancel");
    }

    private boolean cancel(long id) {
        dagRunExecutor.cancel(id);
        return true;
    }
}
