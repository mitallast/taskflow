package org.github.mitallast.taskflow.rest.handler;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import org.github.mitallast.taskflow.operation.OperationService;
import org.github.mitallast.taskflow.rest.RestController;

public class OperationController {

    @Inject
    public OperationController(RestController controller, OperationService operationService) {
        controller.handler(operationService::operations)
            .response(controller.response().json())
            .handle(HttpMethod.GET, "api/operation");
    }
}
