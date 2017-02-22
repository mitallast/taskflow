package org.github.mitallast.taskflow.executor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import org.github.mitallast.taskflow.common.Immutable;
import org.github.mitallast.taskflow.dag.*;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;

import static org.github.mitallast.taskflow.common.Immutable.*;
import static org.github.mitallast.taskflow.dag.TaskRunStatus.*;


public final class DagRunState {

    private final Dag dag;
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final DagRun dagRun;

    // task token => task
    private final ImmutableMap<String, Task> tokenTaskMap;
    // task id => task
    private final ImmutableMap<Long, Task> idTaskMap;

    // task id => list of task run
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final ImmutableListMultimap<Long, TaskRun> idTaskRunMap;
    // task id => last task run
    private final ImmutableMap<Long, TaskRun> idTaskRunLatestMap;

    public DagRunState(Dag dag, DagRun dagRun) {
        Preconditions.checkNotNull(dag);
        Preconditions.checkNotNull(dagRun);
        Preconditions.checkArgument(!dag.tasks().isEmpty(), "Empty DAG");

        this.dag = dag;
        this.dagRun = dagRun;

        idTaskMap = group(dag.tasks(), Task::id);
        tokenTaskMap = group(dag.tasks(), Task::token);

        idTaskRunMap = groupSorted(dagRun.tasks(), task -> task.task().id());
        idTaskRunLatestMap = reduce(idTaskRunMap, Immutable::last);

        buildTaskDag();

        for (Task task : dag.tasks()) {
            Preconditions.checkArgument(idTaskRunMap.containsKey(task.id()), "Task " + task.id() + " does not contain runs");
        }
        for (TaskRun taskRun : dagRun.tasks()) {
            Preconditions.checkArgument(idTaskMap.containsKey(taskRun.task().id()), "task runs illegal references to task " + taskRun.id());
        }
        // validate task run state - only one running|pending per task
        for (Long id : idTaskRunMap.keySet()) {
            TaskRun lastRun = null;
            ImmutableList<TaskRun> taskRuns = idTaskRunMap.get(id);
            for (TaskRun taskRun : taskRuns) {
                if (lastRun != null) {
                    Preconditions.checkArgument(lastRun.status() != PENDING, "Prev task run in pending state");
                    Preconditions.checkArgument(lastRun.status() != RUNNING, "Prev task run in running state");
                    Preconditions.checkArgument(lastRun.status() != CANCELED, "Prev task run in canceled state");
                    Preconditions.checkNotNull(lastRun.finishDate());
                    if (taskRun.status() != PENDING) {
                        Preconditions.checkNotNull(taskRun.startDate());
                        Preconditions.checkArgument(lastRun.finishDate().compareTo(taskRun.startDate()) <= 0, "Task run time conflict found");
                    }
                }
                lastRun = taskRun;
            }
            Preconditions.checkNotNull(lastRun, "No run found for task " + id);
        }
    }

    public ImmutableCollection<TaskRun> lastTaskRuns() {
        return idTaskRunLatestMap.values();
    }

    public boolean hasFailedOutOfRetry() {
        for (Task task : dag.tasks()) {
            ImmutableList<TaskRun> taskRuns = idTaskRunMap.get(task.id());
            if (taskRuns.stream().map(TaskRun::status).filter(FAILED::equals).count() >= task.retry()) {
                return true;
            }
        }
        return false;
    }

    public boolean hasLastRunCanceled() {
        for (TaskRun taskRun : idTaskRunLatestMap.values()) {
            if (taskRun.status() == CANCELED) {
                return true;
            }
        }
        return false;
    }

    public boolean hasUnfinished() {
        for (TaskRun taskRun : idTaskRunLatestMap.values()) {
            if (taskRun.status() == PENDING) {
                return true;
            }
            if (taskRun.status() == RUNNING) {
                return true;
            }
        }
        return false;
    }

    public TaskRunStatus taskRunDependsStatus(TaskRun taskRun) {
        Task task = idTaskMap.get(taskRun.task().id());
        return taskRunDependsStatus(task);
    }

    public TaskRunStatus taskRunDependsStatus(Task task) {
        ImmutableList<Task> depends = depends(task);
        return taskRunDependsStatus(depends);
    }

    public Task task(TaskRun taskRun) {
        return idTaskMap.get(taskRun.task().id());
    }

    public ImmutableList<TaskRun> taskRuns(Task task) {
        return idTaskRunMap.get(task.id());
    }

    private TaskRunStatus taskRunDependsStatus(ImmutableList<Task> depends) {
        boolean hasPending = false;
        boolean hasRunning = false;

        for (Task dependTask : depends) {
            // ignore success
            TaskRunStatus taskRunStatus = taskRun(dependTask).status();
            switch (taskRunStatus) {
                case PENDING:
                    hasPending = true;
                    break;
                case RUNNING:
                    hasRunning = true;
                    break;
                case SUCCESS:
                    //ignore
                    break;
                default:
                    throw new IllegalStateException("Illegal task run state: " + taskRunStatus);
            }
        }
        if (hasRunning) {
            return RUNNING;
        }
        if (hasPending) {
            return PENDING;
        }

        return TaskRunStatus.SUCCESS;
    }

    public TaskRun taskRun(Task task) {
        return idTaskRunLatestMap.get(task.id());
    }

    public ImmutableList<Task> depends(TaskRun taskRun) {
        Task task = idTaskMap.get(taskRun.task().id());
        return depends(task);
    }

    public ImmutableList<Task> depends(Task task) {
        ImmutableList.Builder<Task> builder = ImmutableList.builder();
        for (String dependsToken : task.depends()) {
            builder.add(tokenTaskMap.get(dependsToken));
        }
        return builder.build();
    }

    private DirectedAcyclicGraph<String, DefaultEdge> buildTaskDag() {
        DirectedAcyclicGraph<String, DefaultEdge> graph = new DirectedAcyclicGraph<>(DefaultEdge.class);
        for (Task task : dag.tasks()) {
            graph.addVertex(task.token());
        }
        for (Task task : dag.tasks()) {
            for (String depends : task.depends()) {
                graph.addEdge(depends, task.token());
            }
        }
        return graph;
    }
}
