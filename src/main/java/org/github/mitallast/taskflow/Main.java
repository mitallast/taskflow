package org.github.mitallast.taskflow;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.github.mitallast.taskflow.common.component.ComponentModule;
import org.github.mitallast.taskflow.common.component.LifecycleService;
import org.github.mitallast.taskflow.common.component.ModulesBuilder;
import org.github.mitallast.taskflow.common.json.JsonModule;
import org.github.mitallast.taskflow.dag.*;
import org.github.mitallast.taskflow.operation.*;
import org.github.mitallast.taskflow.persistence.PersistenceModule;
import org.github.mitallast.taskflow.rest.RestModule;
import org.github.mitallast.taskflow.scheduler.SchedulerModule;

import java.io.IOException;

public class Main {
    private static final Object mutex = new Object();

    public static void main(String... args) throws Exception {
        Config config = ConfigFactory.load();

        ModulesBuilder modules = new ModulesBuilder();
        modules.add(new ComponentModule(config));
        modules.add(new JsonModule());
        modules.add(new PersistenceModule());
        modules.add(new OperationModule());
        modules.add(new DagModule());
        modules.add(new SchedulerModule());
        modules.add(new RestModule());

        Injector injector = modules.createInjector();
        LifecycleService lifecycleService = injector.getInstance(LifecycleService.class);

        lifecycleService.start();

        DagPersistenceService dagPersistence = injector.getInstance(DagPersistenceService.class);

        dagPersistence.createDag(new Dag(
            "test_dag",
            new Task("test_task_1", ImmutableSet.of(), "dummy", new OperationCommand(ConfigFactory.empty(), new OperationEnvironment()))
        ));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                synchronized (mutex) {
                    mutex.notify();
                }
                lifecycleService.stop();
                lifecycleService.close();
            } catch (IOException e) {
                e.printStackTrace(System.err);
            }
        }));

        synchronized (mutex) {
            mutex.wait();
        }
    }
}
