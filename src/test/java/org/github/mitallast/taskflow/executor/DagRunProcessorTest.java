package org.github.mitallast.taskflow.executor;

import com.google.common.collect.ImmutableList;
import com.typesafe.config.ConfigFactory;
import org.github.mitallast.taskflow.common.BaseTest;
import org.github.mitallast.taskflow.dag.*;
import org.github.mitallast.taskflow.operation.OperationCommand;
import org.github.mitallast.taskflow.operation.OperationEnvironment;
import org.github.mitallast.taskflow.executor.command.*;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static com.google.common.collect.ImmutableSet.of;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.*;

@RunWith(Parameterized.class)
public class DagRunProcessorTest extends BaseTest {

    private static final DagRunProcessor processor = new DagRunProcessor();

    private static final DateTime created = new DateTime();

    private static final OperationCommand command = new OperationCommand(
        ConfigFactory.empty(),
        new OperationEnvironment()
    );

    private static final Task taskA = new Task(1, 1, "A", of(), 3, "dummy", command);
    private static final Task taskB = new Task(2, 1, "B", of(), 3, "dummy", command);
    private static final Task taskC = new Task(3, 1, "C", of(), 3, "dummy", command);
    private static final Task taskD = new Task(4, 1, "D", of(), 3, "dummy", command);
    private static final Task taskE = new Task(5, 1, "E", of(), 3, "dummy", command);

    private static final ImmutableList<Task> tasks = ImmutableList.of(taskA, taskB, taskC, taskD, taskE);

    private static final Dag dag = new Dag(1, 1, "dag", tasks);

    private static final TaskRun taskRunA = new TaskRun(1, 1, 1, 1, created, null, null, TaskRunStatus.PENDING, null);
    private static final TaskRun taskRunB = new TaskRun(2, 1, 2, 1, created, null, null, TaskRunStatus.PENDING, null);
    private static final TaskRun taskRunC = new TaskRun(3, 1, 3, 1, created, null, null, TaskRunStatus.PENDING, null);
    private static final TaskRun taskRunD = new TaskRun(4, 1, 4, 1, created, null, null, TaskRunStatus.PENDING, null);
    private static final TaskRun taskRunE = new TaskRun(5, 1, 5, 1, created, null, null, TaskRunStatus.PENDING, null);

    private static final TaskRun taskRunA2 = new TaskRun(6, 1, 1, 1, created, null, null, TaskRunStatus.PENDING, null);
    private static final TaskRun taskRunA3 = new TaskRun(7, 1, 1, 1, created, null, null, TaskRunStatus.PENDING, null);

    private static final ImmutableList<TaskRun> taskRuns = ImmutableList.of(taskRunA, taskRunB, taskRunC, taskRunD, taskRunE);

    private static final DagRun dagRun = new DagRun(1, 1, created, null, null, DagRunStatus.PENDING, taskRuns);

    @Parameterized.Parameters
    public static Collection<Object[]> cases() {
        return Arrays.asList(new Object[][]{
            {dag, dagRun, new StartDagRunCommand(dagRun)},
            // check no deps
            {dag, dagRun.start(), new ExecuteTaskRunCommand(taskRunA)},
            {dag, dagRun.start().start(taskRunA), new ExecuteTaskRunCommand(taskRunB)},
            {dag, dagRun.start().start(taskRunA, taskRunB), new ExecuteTaskRunCommand(taskRunC)},
            {dag, dagRun.start().start(taskRunA, taskRunB, taskRunC), new ExecuteTaskRunCommand(taskRunD)},
            {dag, dagRun.start().start(taskRunA, taskRunB, taskRunC, taskRunD), new ExecuteTaskRunCommand(taskRunE)},
            {dag, dagRun.start().start(taskRunA, taskRunB, taskRunC, taskRunD, taskRunE), new AwaitCommand(dagRun)},
            {dag, dagRun.start().success(taskRunA), new ExecuteTaskRunCommand(taskRunB)},
            {dag, dagRun.start().success(taskRunA, taskRunB), new ExecuteTaskRunCommand(taskRunC)},
            {dag, dagRun.start().success(taskRunA, taskRunB, taskRunC), new ExecuteTaskRunCommand(taskRunD)},
            {dag, dagRun.start().success(taskRunA, taskRunB, taskRunC, taskRunD), new ExecuteTaskRunCommand(taskRunE)},
            {dag, dagRun.start().success(taskRunA, taskRunB, taskRunC, taskRunD, taskRunE), new SuccessDagRunCommand(dagRun)},

            // check dag complete
            {dag, dagRun.start().success(), new CompleteDagRunCommand(dagRun)},

            // check deps 1 depth
            {dag.update(taskA.depend("B")), dagRun.start(), new ExecuteTaskRunCommand(taskRunB)},
            {dag.update(taskA.depend("C")), dagRun.start(), new ExecuteTaskRunCommand(taskRunB)},
            {dag.update(taskA.depend("D")), dagRun.start(), new ExecuteTaskRunCommand(taskRunB)},
            {dag.update(taskA.depend("E")), dagRun.start(), new ExecuteTaskRunCommand(taskRunB)},
            {dag.update(taskA.depend("B", "C")), dagRun.start(), new ExecuteTaskRunCommand(taskRunB)},
            {dag.update(taskA.depend("B", "D")), dagRun.start(), new ExecuteTaskRunCommand(taskRunB)},
            {dag.update(taskA.depend("B", "E")), dagRun.start(), new ExecuteTaskRunCommand(taskRunB)},
            {dag.update(taskA.depend("B", "C", "D")), dagRun.start(), new ExecuteTaskRunCommand(taskRunB)},
            // check deps 1 depth finished
            {dag.update(taskA.depend("B")), dagRun.start().success(taskRunB), new ExecuteTaskRunCommand(taskRunA)},
            {dag.update(taskA.depend("B", "C")), dagRun.start().success(taskRunB), new ExecuteTaskRunCommand(taskRunC)},
            {dag.update(taskA.depend("B", "C")), dagRun.start().success(taskRunB, taskRunC), new ExecuteTaskRunCommand(taskRunA)},
            {dag.update(taskA.depend("B", "C", "D")), dagRun.start().success(taskRunB), new ExecuteTaskRunCommand(taskRunC)},
            {dag.update(taskA.depend("B", "C", "D")), dagRun.start().success(taskRunB, taskRunC), new ExecuteTaskRunCommand(taskRunD)},
            {dag.update(taskA.depend("B", "C", "D")), dagRun.start().success(taskRunB, taskRunC, taskRunD), new ExecuteTaskRunCommand(taskRunA)},

            // check deps 2 depth
            {dag.update(taskA.depend("B")).update(taskB.depend("C")), dagRun.start(), new ExecuteTaskRunCommand(taskRunC)},
            {dag.update(taskA.depend("B")).update(taskB.depend("D")), dagRun.start(), new ExecuteTaskRunCommand(taskRunC)},
            {dag.update(taskA.depend("B")).update(taskB.depend("E")), dagRun.start(), new ExecuteTaskRunCommand(taskRunC)},

            // check retry
            {dag, dagRun.start().failure(taskRunA), new RetryTaskRunCommand(taskRunA)},
            {dag, dagRun.start().failure(taskRunA).retry(taskRunA2), new ExecuteTaskRunCommand(taskRunA2)},
            {dag, dagRun.start().failure(taskRunA).retry(taskRunA2).start(taskRunA2), new ExecuteTaskRunCommand(taskRunB)},
            {dag, dagRun.start().failure(taskRunA, taskRunB).retry(taskRunA2), new RetryTaskRunCommand(taskRunB)},
            {dag, dagRun.start().failure(taskRunA, taskRunB, taskRunC, taskRunD, taskRunE).retry(taskRunA2), new RetryTaskRunCommand(taskRunB)},
            {dag, dagRun.start().failure(taskRunA).retry(taskRunA2).retry(taskRunA3).failure(taskRunA2, taskRunA3), new CancelTaskRunCommand(taskRunB)},
            {dag, dagRun.start().failure(taskRunA).retry(taskRunA2).retry(taskRunA3).failure(taskRunA2, taskRunA3).success(taskRunB,taskRunC,taskRunD,taskRunE), new FailedDagRunCommand(dagRun)},

            // check cancel
            {dag, dagRun.start().cancel(taskRunA), new CancelTaskRunCommand(taskRunB)},
            {dag, dagRun.start().cancel(taskRunA, taskRunB), new CancelTaskRunCommand(taskRunC)},
            {dag, dagRun.start().cancel(taskRunA, taskRunB, taskRunC), new CancelTaskRunCommand(taskRunD)},
            {dag, dagRun.start().cancel(taskRunA, taskRunB, taskRunC, taskRunD), new CancelTaskRunCommand(taskRunE)},
            {dag, dagRun.start().cancel(taskRunA, taskRunB, taskRunC, taskRunD, taskRunE), new CancelDagRunCommand(dagRun)},
            {dag, dagRun.start().cancel(taskRunA, taskRunB, taskRunC, taskRunD, taskRunE), new CancelDagRunCommand(dagRun)},
            // check cancel if one running
            {dag, dagRun.start().cancel(taskRunA).start(taskRunB), new CancelTaskRunCommand(taskRunB)},
            {dag, dagRun.start().cancel(taskRunA, taskRunB, taskRunC).start(taskRunD), new CancelTaskRunCommand(taskRunD)},
            {dag, dagRun.start().cancel(taskRunA, taskRunB, taskRunC, taskRunD).start(taskRunE), new CancelTaskRunCommand(taskRunE)},
            // check cancel if one success
            {dag, dagRun.start().cancel(taskRunA).success(taskRunB), new CancelTaskRunCommand(taskRunC)},
            {dag, dagRun.start().cancel(taskRunA, taskRunB, taskRunC).success(taskRunD), new CancelTaskRunCommand(taskRunE)},
            {dag, dagRun.start().cancel(taskRunA, taskRunB, taskRunC, taskRunD).success(taskRunE), new CancelDagRunCommand(dagRun)},
            // check cancel if one failed
            {dag, dagRun.start().cancel(taskRunA).failure(taskRunB), new CancelTaskRunCommand(taskRunC)},
            {dag, dagRun.start().cancel(taskRunA, taskRunB, taskRunC).failure(taskRunD), new CancelTaskRunCommand(taskRunE)},
            {dag, dagRun.start().cancel(taskRunA, taskRunB, taskRunC, taskRunD).failure(taskRunE), new CancelDagRunCommand(dagRun)},
        });
    }

    private final Dag testDag;
    private final DagRun testDagRun;
    private final Command expected;

    public DagRunProcessorTest(Dag dag, DagRun dagRun, Command expected) {
        this.testDag = dag;
        this.testDagRun = dagRun;
        this.expected = expected;
    }

    @Test
    public void testScheduler() throws Exception {
        Command cmd = processor.process(testDag, testDagRun);
        assertThat(cmd, is(instanceOf(expected.getClass())));

        if (expected instanceof TaskRunCommand) {
            assertThat(((TaskRunCommand) cmd).taskRun().id(), is(equalTo(((TaskRunCommand) expected).taskRun().id())));
        }
    }
}
