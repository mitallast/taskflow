package org.github.mitallast.taskflow;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.github.mitallast.taskflow.common.component.ComponentModule;
import org.github.mitallast.taskflow.common.component.LifecycleService;
import org.github.mitallast.taskflow.common.component.ModulesBuilder;
import org.github.mitallast.taskflow.dag.Dag;
import org.github.mitallast.taskflow.dag.DagModule;
import org.github.mitallast.taskflow.dag.DagPersistenceService;
import org.github.mitallast.taskflow.dag.Task;
import org.github.mitallast.taskflow.operation.OperationCommand;
import org.github.mitallast.taskflow.operation.OperationEnvironment;
import org.github.mitallast.taskflow.operation.OperationModule;
import org.github.mitallast.taskflow.persistence.PersistenceModule;

import static org.github.mitallast.taskflow.dag.TaskRun_.dag;

public class Main {
    private static final Object mutex = new Object();

    public static void main(String... args) throws Exception {
        Config config = ConfigFactory.load();

        ModulesBuilder modules = new ModulesBuilder();
        modules.add(new ComponentModule(config));
        modules.add(new PersistenceModule());
        modules.add(new OperationModule());
        modules.add(new DagModule());

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

        dagPersistence.createDagRun(dag1);
        dagPersistence.createDagRun(dag2);
        dagPersistence.createDagRun(dag3);
        dagPersistence.createDagRun(dag4);

        lifecycleService.stop();
        lifecycleService.close();
//
//        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//            try {
//                synchronized (mutex) {
//                    mutex.notify();
//                }
//                lifecycleService.stop();
//                lifecycleService.close();
//            } catch (IOException e) {
//                e.printStackTrace(System.err);
//            }
//        }));
//
//        synchronized (mutex) {
//            mutex.wait();
//        }
    }
}
