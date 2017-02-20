package org.github.mitallast.taskflow.rest.handler;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.github.mitallast.taskflow.dag.DagPersistenceService;
import org.github.mitallast.taskflow.dag.DagSchedule;
import org.github.mitallast.taskflow.rest.RestController;
import org.github.mitallast.taskflow.scheduler.DagSchedulerService;

public class DagScheduleController {

    @Inject
    public DagScheduleController(
        RestController controller,
        DagPersistenceService persistenceService,
        DagSchedulerService schedulerService
    ) {

        controller.handler(persistenceService::findDagSchedules)
            .response(controller.response().json())
            .handle(HttpMethod.GET, "api/dag/schedule");

        controller.handler(persistenceService::findDagSchedule)
            .param(controller.param().string("token"))
            .response(controller.response().optionalJson())
            .handle(HttpMethod.GET, "api/dag/token/{token}/schedule");

        controller.handler(schedulerService::markDagScheduleEnabled)
            .param(controller.param().string("token"))
            .response((request, result) -> {
                if (result) request.response().status(HttpResponseStatus.NO_CONTENT).empty();
                else request.response().status(HttpResponseStatus.METHOD_NOT_ALLOWED).empty();
            })
            .handle(HttpMethod.PUT, "api/dag/token/{token}/schedule/enable");

        controller.handler(schedulerService::markDagScheduleDisabled)
            .param(controller.param().string("token"))
            .response((request, result) -> {
                if (result) request.response().status(HttpResponseStatus.NO_CONTENT).empty();
                else request.response().status(HttpResponseStatus.METHOD_NOT_ALLOWED).empty();
            })
            .handle(HttpMethod.PUT, "api/dag/token/{token}/schedule/disable");

        controller.handler(schedulerService::update)
            .param(controller.param().json(DagSchedule.class))
            .response(controller.response().maybeJson())
            .handle(HttpMethod.PUT, "api/dag/schedule");
    }
}
