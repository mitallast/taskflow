package org.github.mitallast.taskflow.operation;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.github.mitallast.taskflow.common.component.AbstractComponent;

import java.util.Set;

public class OperationService extends AbstractComponent {

    private final ImmutableMap<String, Operation> operationsMap;

    @Inject
    public OperationService(Config config, Set<Operation> operations) {
        super(config.getConfig("operation"), OperationService.class);

        ImmutableMap.Builder<String, Operation> builder = ImmutableMap.builder();
        operations.forEach(operation -> builder.put(operation.id(), operation));
        operationsMap = builder.build();
    }

    public Operation operation(String id) {
        if (!operationsMap.containsKey(id)) {
            throw new IllegalArgumentException("Operation not found: " + id);
        }
        return operationsMap.get(id);
    }

    public ImmutableSet<String> operations() {
        return operationsMap.keySet();
    }

    public boolean contains(String id) {
        return operationsMap.containsKey(id);
    }
}
