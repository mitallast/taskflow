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

    private abstract static class TestCase {
        Task taskA() {
            return new Task(1, 1, "A", of(), 3, "dummy", command);
        }

        Task taskB() {
            return new Task(2, 1, "B", of(), 3, "dummy", command);
        }

        Task taskC() {
            return new Task(3, 1, "C", of(), 3, "dummy", command);
        }

        Task taskD() {
            return new Task(4, 1, "D", of(), 3, "dummy", command);
        }

        Task taskE() {
            return new Task(5, 1, "E", of(), 3, "dummy", command);
        }

        ImmutableList<Task> tasks() {
            return ImmutableList.of(taskA(), taskB(), taskC(), taskD(), taskE());
        }

        Dag dag() {
            return new Dag(1, 1, "dag", tasks());
        }

        TaskRun taskRunA() {
            return new TaskRun(1, 1, taskA(), 1, created, null, null, TaskRunStatus.PENDING, null);
        }

        TaskRun taskRunB() {
            return new TaskRun(2, 1, taskB(), 1, created, null, null, TaskRunStatus.PENDING, null);
        }

        TaskRun taskRunC() {
            return new TaskRun(3, 1, taskC(), 1, created, null, null, TaskRunStatus.PENDING, null);
        }

        TaskRun taskRunD() {
            return new TaskRun(4, 1, taskD(), 1, created, null, null, TaskRunStatus.PENDING, null);
        }

        TaskRun taskRunE() {
            return new TaskRun(5, 1, taskE(), 1, created, null, null, TaskRunStatus.PENDING, null);
        }

        TaskRun taskRunA2() {
            return new TaskRun(6, 1, taskA(), 1, created, null, null, TaskRunStatus.PENDING, null);
        }

        TaskRun taskRunA3() {
            return new TaskRun(7, 1, taskA(), 1, created, null, null, TaskRunStatus.PENDING, null);
        }

        ImmutableList<TaskRun> taskRuns() {
            return ImmutableList.of(taskRunA(), taskRunB(), taskRunC(), taskRunD(), taskRunE());
        }

        DagRun dagRun() {
            return new DagRun(1, dag(), created, null, null, DagRunStatus.PENDING, taskRuns());
        }

        abstract Command expected();
    }

    @Parameterized.Parameters
    public static Collection<TestCase> cases() {
        return Arrays.asList(
            // check no deps
            new TestCase() {
                Command expected() {
                    return new StartDagRunCommand(dagRun());
                }
            },
            new TestCase() {
                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new ExecuteTaskRunCommand(taskRunA());
                }
            },
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new ExecuteTaskRunCommand(taskRunB());
                }
            },
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start();
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new ExecuteTaskRunCommand(taskRunC());
                }
            },
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start();
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start();
                }

                TaskRun taskRunC() {
                    return super.taskRunC().start();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new ExecuteTaskRunCommand(taskRunD());
                }
            },
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start();
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start();
                }

                TaskRun taskRunC() {
                    return super.taskRunC().start();
                }

                TaskRun taskRunD() {
                    return super.taskRunD().start();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new ExecuteTaskRunCommand(taskRunE());
                }
            },
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start();
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start();
                }

                TaskRun taskRunC() {
                    return super.taskRunC().start();
                }

                TaskRun taskRunD() {
                    return super.taskRunD().start();
                }

                TaskRun taskRunE() {
                    return super.taskRunE().start();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new AwaitCommand(dagRun());
                }
            },
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start().success();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new ExecuteTaskRunCommand(taskRunB());
                }
            },
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start().success();
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start().success();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new ExecuteTaskRunCommand(taskRunC());
                }
            },
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start().success();
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start().success();
                }

                TaskRun taskRunC() {
                    return super.taskRunC().start().success();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new ExecuteTaskRunCommand(taskRunD());
                }
            },
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start().success();
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start().success();
                }

                TaskRun taskRunC() {
                    return super.taskRunC().start().success();
                }

                TaskRun taskRunD() {
                    return super.taskRunD().start().success();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new ExecuteTaskRunCommand(taskRunE());
                }
            },
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start().success();
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start().success();
                }

                TaskRun taskRunC() {
                    return super.taskRunC().start().success();
                }

                TaskRun taskRunD() {
                    return super.taskRunD().start().success();
                }

                TaskRun taskRunE() {
                    return super.taskRunE().start().success();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new SuccessDagRunCommand(dagRun());
                }
            },
            // check dag complete
            new TestCase() {
                DagRun dagRun() {
                    return super.dagRun().start().success();
                }

                Command expected() {
                    return new CompleteDagRunCommand(dagRun());
                }
            },
            // check deps 1 depth
            new TestCase() {
                Task taskA() {
                    return super.taskA().depend("B");
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new ExecuteTaskRunCommand(taskRunB());
                }
            },
            new TestCase() {
                Task taskA() {
                    return super.taskA().depend("C");
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new ExecuteTaskRunCommand(taskRunB());
                }
            },
            new TestCase() {
                Task taskA() {
                    return super.taskA().depend("D");
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new ExecuteTaskRunCommand(taskRunB());
                }
            },
            new TestCase() {
                Task taskA() {
                    return super.taskA().depend("E");
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new ExecuteTaskRunCommand(taskRunB());
                }
            },
            new TestCase() {
                Task taskA() {
                    return super.taskA().depend("B", "C");
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new ExecuteTaskRunCommand(taskRunB());
                }
            },
            new TestCase() {
                Task taskA() {
                    return super.taskA().depend("B", "D");
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new ExecuteTaskRunCommand(taskRunB());
                }
            },
            new TestCase() {
                Task taskA() {
                    return super.taskA().depend("B", "E");
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new ExecuteTaskRunCommand(taskRunB());
                }
            },
            new TestCase() {
                Task taskA() {
                    return super.taskA().depend("B", "C", "D");
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new ExecuteTaskRunCommand(taskRunB());
                }
            },
            // check deps 1 depth finished
            new TestCase() {
                Task taskA() {
                    return super.taskA().depend("B");
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start().success();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new ExecuteTaskRunCommand(taskRunA());
                }
            },
            new TestCase() {
                Task taskA() {
                    return super.taskA().depend("B", "C");
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start().success();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new ExecuteTaskRunCommand(taskRunC());
                }
            },
            new TestCase() {
                Task taskA() {
                    return super.taskA().depend("B", "C");
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start().success();
                }

                TaskRun taskRunC() {
                    return super.taskRunC().start().success();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new ExecuteTaskRunCommand(taskRunA());
                }
            },
            new TestCase() {
                Task taskA() {
                    return super.taskA().depend("B", "C", "D");
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start().success();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new ExecuteTaskRunCommand(taskRunC());
                }
            },
            new TestCase() {
                Task taskA() {
                    return super.taskA().depend("B", "C", "D");
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start().success();
                }

                TaskRun taskRunC() {
                    return super.taskRunC().start().success();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new ExecuteTaskRunCommand(taskRunD());
                }
            },
            new TestCase() {
                Task taskA() {
                    return super.taskA().depend("B", "C", "D");
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start().success();
                }

                TaskRun taskRunC() {
                    return super.taskRunC().start().success();
                }

                TaskRun taskRunD() {
                    return super.taskRunD().start().success();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new ExecuteTaskRunCommand(taskRunA());
                }
            },

            // check deps 2 depth

            new TestCase() {
                Task taskA() {
                    return super.taskA().depend("B");
                }

                Task taskB() {
                    return super.taskB().depend("C");
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new ExecuteTaskRunCommand(taskRunC());
                }
            },
            new TestCase() {
                Task taskA() {
                    return super.taskA().depend("B");
                }

                Task taskB() {
                    return super.taskB().depend("D");
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new ExecuteTaskRunCommand(taskRunC());
                }
            },
            new TestCase() {
                Task taskA() {
                    return super.taskA().depend("B");
                }

                Task taskB() {
                    return super.taskB().depend("E");
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new ExecuteTaskRunCommand(taskRunC());
                }
            },
            // check retry
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start().failure();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new RetryTaskRunCommand(taskRunA());
                }
            },
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start().failure();
                }

                ImmutableList<TaskRun> taskRuns() {
                    return ImmutableList.<TaskRun>builder()
                        .addAll(super.taskRuns())
                        .add(taskRunA2())
                        .build();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new ExecuteTaskRunCommand(taskRunA2());
                }
            },
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start().failure();
                }

                TaskRun taskRunA2() {
                    return super.taskRunA2().start();
                }

                ImmutableList<TaskRun> taskRuns() {
                    return ImmutableList.<TaskRun>builder()
                        .addAll(super.taskRuns())
                        .add(taskRunA2())
                        .build();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new ExecuteTaskRunCommand(taskRunB());
                }
            },
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start().failure();
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start().failure();
                }

                TaskRun taskRunA2() {
                    return super.taskRunA2().start();
                }

                ImmutableList<TaskRun> taskRuns() {
                    return ImmutableList.<TaskRun>builder()
                        .addAll(super.taskRuns())
                        .add(taskRunA2())
                        .build();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new RetryTaskRunCommand(taskRunB());
                }
            },
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start().failure();
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start().failure();
                }

                TaskRun taskRunC() {
                    return super.taskRunC().start().failure();
                }

                TaskRun taskRunD() {
                    return super.taskRunD().start().failure();
                }

                TaskRun taskRunE() {
                    return super.taskRunE().start().failure();
                }

                TaskRun taskRunA2() {
                    return super.taskRunA2().start();
                }

                ImmutableList<TaskRun> taskRuns() {
                    return ImmutableList.<TaskRun>builder()
                        .addAll(super.taskRuns())
                        .add(taskRunA2())
                        .build();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new RetryTaskRunCommand(taskRunB());
                }
            },
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start().failure();
                }

                TaskRun taskRunA2() {
                    return super.taskRunA2().start().failure();
                }

                TaskRun taskRunA3() {
                    return super.taskRunA3().start().failure();
                }

                ImmutableList<TaskRun> taskRuns() {
                    return ImmutableList.<TaskRun>builder()
                        .addAll(super.taskRuns())
                        .add(taskRunA2())
                        .add(taskRunA3())
                        .build();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new CancelTaskRunCommand(taskRunB());
                }
            },
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start().failure();
                }

                TaskRun taskRunA2() {
                    return super.taskRunA2().start().failure();
                }

                TaskRun taskRunA3() {
                    return super.taskRunA3().start().failure();
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start().success();
                }

                TaskRun taskRunC() {
                    return super.taskRunC().start().success();
                }

                TaskRun taskRunD() {
                    return super.taskRunD().start().success();
                }

                TaskRun taskRunE() {
                    return super.taskRunE().start().success();
                }

                ImmutableList<TaskRun> taskRuns() {
                    return ImmutableList.<TaskRun>builder()
                        .addAll(super.taskRuns())
                        .add(taskRunA2())
                        .add(taskRunA3())
                        .build();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new FailedDagRunCommand(dagRun());
                }
            },
            // check cancel
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start().cancel();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new CancelTaskRunCommand(taskRunB());
                }
            },
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start().cancel();
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start().cancel();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new CancelTaskRunCommand(taskRunC());
                }
            },
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start().cancel();
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start().cancel();
                }

                TaskRun taskRunC() {
                    return super.taskRunC().start().cancel();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new CancelTaskRunCommand(taskRunD());
                }
            },
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start().cancel();
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start().cancel();
                }

                TaskRun taskRunC() {
                    return super.taskRunC().start().cancel();
                }

                TaskRun taskRunD() {
                    return super.taskRunD().start().cancel();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new CancelTaskRunCommand(taskRunE());
                }
            },
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start().cancel();
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start().cancel();
                }

                TaskRun taskRunC() {
                    return super.taskRunC().start().cancel();
                }

                TaskRun taskRunD() {
                    return super.taskRunD().start().cancel();
                }

                TaskRun taskRunE() {
                    return super.taskRunE().start().cancel();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new CancelDagRunCommand(dagRun());
                }
            },
            // check cancel if one running
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start().cancel();
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new CancelTaskRunCommand(taskRunB());
                }
            },
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start().cancel();
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start().cancel();
                }

                TaskRun taskRunC() {
                    return super.taskRunC().start();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new CancelTaskRunCommand(taskRunC());
                }
            },
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start().cancel();
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start().cancel();
                }

                TaskRun taskRunC() {
                    return super.taskRunC().start().cancel();
                }

                TaskRun taskRunD() {
                    return super.taskRunD().start();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new CancelTaskRunCommand(taskRunD());
                }
            },
            // check cancel if one success
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start().cancel();
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start().success();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new CancelTaskRunCommand(taskRunC());
                }
            },
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start().cancel();
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start().cancel();
                }

                TaskRun taskRunC() {
                    return super.taskRunC().start().success();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new CancelTaskRunCommand(taskRunD());
                }
            },
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start().cancel();
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start().cancel();
                }

                TaskRun taskRunC() {
                    return super.taskRunC().start().cancel();
                }

                TaskRun taskRunD() {
                    return super.taskRunD().start().success();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new CancelTaskRunCommand(taskRunE());
                }
            },
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start().cancel();
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start().cancel();
                }

                TaskRun taskRunC() {
                    return super.taskRunC().start().cancel();
                }

                TaskRun taskRunD() {
                    return super.taskRunD().start().cancel();
                }

                TaskRun taskRunE() {
                    return super.taskRunE().start().success();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new CancelDagRunCommand(dagRun());
                }
            },
//            // check cancel if one failed
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start().cancel();
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start().failure();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new CancelTaskRunCommand(taskRunC());
                }
            },
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start().cancel();
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start().cancel();
                }

                TaskRun taskRunC() {
                    return super.taskRunC().start().failure();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new CancelTaskRunCommand(taskRunD());
                }
            },
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start().cancel();
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start().cancel();
                }

                TaskRun taskRunC() {
                    return super.taskRunC().start().cancel();
                }

                TaskRun taskRunD() {
                    return super.taskRunD().start().failure();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new CancelTaskRunCommand(taskRunE());
                }
            },
            new TestCase() {
                TaskRun taskRunA() {
                    return super.taskRunA().start().cancel();
                }

                TaskRun taskRunB() {
                    return super.taskRunB().start().cancel();
                }

                TaskRun taskRunC() {
                    return super.taskRunC().start().cancel();
                }

                TaskRun taskRunD() {
                    return super.taskRunD().start().cancel();
                }

                TaskRun taskRunE() {
                    return super.taskRunE().start().failure();
                }

                DagRun dagRun() {
                    return super.dagRun().start();
                }

                Command expected() {
                    return new CancelDagRunCommand(dagRun());
                }
            }
        );
    }

    private final DagRun testDagRun;
    private final Command expected;

    public DagRunProcessorTest(TestCase testCase) {
        this.testDagRun = testCase.dagRun();
        this.expected = testCase.expected();
    }

    @Test
    public void testScheduler() throws Exception {
        Command cmd = processor.process(testDagRun);
        assertThat(cmd, is(instanceOf(expected.getClass())));

        if (expected instanceof TaskRunCommand) {
            assertThat(((TaskRunCommand) cmd).taskRun().id(), is(equalTo(((TaskRunCommand) expected).taskRun().id())));
        }
    }
}
