package org.github.mitallast.taskflow;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.github.mitallast.taskflow.common.component.ComponentModule;
import org.github.mitallast.taskflow.common.component.LifecycleService;
import org.github.mitallast.taskflow.common.component.ModulesBuilder;
import org.github.mitallast.taskflow.dag.*;
import org.github.mitallast.taskflow.operation.*;
import org.github.mitallast.taskflow.persistence.PersistenceModule;
import org.github.mitallast.taskflow.rest.RestModule;

import java.io.IOException;

public class Main {
    private static final Object mutex = new Object();

    public static void main(String... args) throws Exception {
        Config config = ConfigFactory.load();

        ModulesBuilder modules = new ModulesBuilder();
        modules.add(new ComponentModule(config));
        modules.add(new PersistenceModule());
        modules.add(new OperationModule());
        modules.add(new DagModule());
        modules.add(new RestModule());

        Injector injector = modules.createInjector();
        LifecycleService lifecycleService = injector.getInstance(LifecycleService.class);

        lifecycleService.start();

        DagPersistenceService dagPersistence = injector.getInstance(DagPersistenceService.class);

        Dag dag1 = dagPersistence.createDag(new Dag(
            "test_dag",
            new Task("test_task_1", ImmutableList.of(), "test_op", new OperationCommand(ConfigFactory.empty(), new OperationEnvironment()))
        ));

        Dag dag2 = dagPersistence.updateDag(dag1);
        Dag dag3 = dagPersistence.updateDag(dag2);
        Dag dag4 = dagPersistence.updateDag(dag3);

        System.out.println(dagPersistence.findLatestDags());
        System.out.println(dagPersistence.findDag(dag1.id()));
        System.out.println(dagPersistence.findDag(dag1.token()));

        DagRun dagRun1 = dagPersistence.createDagRun(dag1);
        DagRun dagRun2 = dagPersistence.createDagRun(dag2);
        DagRun dagRun3 = dagPersistence.createDagRun(dag3);
        DagRun dagRun4 = dagPersistence.createDagRun(dag4);

        dagPersistence.startDagRun(dagRun1.id());
        dagPersistence.startDagRun(dagRun2.id());
        dagPersistence.startDagRun(dagRun3.id());
        dagPersistence.startDagRun(dagRun4.id());

        dagPersistence.startTaskRun(dagRun1.tasks().get(0).id());
        dagPersistence.startTaskRun(dagRun2.tasks().get(0).id());
        dagPersistence.startTaskRun(dagRun3.tasks().get(0).id());
        dagPersistence.startTaskRun(dagRun4.tasks().get(0).id());

        dagPersistence.markTaskRunSuccess(dagRun1.tasks().get(0).id(), new OperationResult(OperationStatus.SUCCESS, "stdout", ""));
        dagPersistence.markTaskRunFailed(dagRun2.tasks().get(0).id(), new OperationResult(OperationStatus.FAILED, "", "stderr"));
        dagPersistence.markTaskRunCanceled(dagRun3.tasks().get(0).id());

        dagPersistence.markDagRunSuccess(dagRun1.id());
        dagPersistence.markDagRunFailed(dagRun2.id());
        dagPersistence.markDagRunCanceled(dagRun3.id());

//        lifecycleService.stop();
//        lifecycleService.close();
//
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
