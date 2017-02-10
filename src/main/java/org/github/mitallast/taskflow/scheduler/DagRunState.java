package org.github.mitallast.taskflow.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import org.github.mitallast.taskflow.dag.*;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;

import java.util.ArrayList;

import static org.github.mitallast.taskflow.dag.TaskRunStatus.*;

public final class DagRunState {

    private final Dag dag;
    private final DagRun dagRun;

    // task token => task
    private final ImmutableMap<String, Task> tokenTaskMap;
    // task id => [task run]
    private final ImmutableListMultimap<Long, TaskRun> idTaskRunMap;

    public DagRunState(Dag dag, DagRun dagRun) {
        Preconditions.checkNotNull(dag);
        Preconditions.checkNotNull(dagRun);
        Preconditions.checkArgument(!dag.tasks().isEmpty(), "Empty DAG");

        this.dag = dag;
        this.dagRun = dagRun;

        ImmutableMap<Long, Task> idTaskMap = buildIdTaskMap();

        tokenTaskMap = buildTokenTaskMap();
        idTaskRunMap = buildIdTaskRunMap();

        buildTaskDag();

        for (Task task : dag.tasks()) {
            Preconditions.checkArgument(idTaskRunMap.containsKey(task.id()), "Task " + task.id() + "does not contain runs");
        }
        for (TaskRun taskRun : dagRun.tasks()) {
            Preconditions.checkArgument(taskRun.status() != CANCELED, "Task run " + taskRun.id() + "canceled ");
            Preconditions.checkArgument(idTaskMap.containsKey(taskRun.taskId()), "task runs illegal references to task " + taskRun.id());
        }
        // validate task run state - only one running|pending per task
        for (Long id : idTaskRunMap.keySet()) {
            TaskRun lastRun = null;
            ImmutableList<TaskRun> taskRuns = idTaskRunMap.get(id);
            for (TaskRun taskRun : taskRuns) {
                if (lastRun != null) {
                    Preconditions.checkArgument(lastRun.status() != PENDING, "Prev task run in pending state");
                    Preconditions.checkArgument(lastRun.status() != RUNNING, "Prev task run in running state");
                    Preconditions.checkNotNull(lastRun.finishDate());
                    if (taskRun.status() != PENDING) {
                        Preconditions.checkNotNull(taskRun.startDate());
                        Preconditions.checkArgument(lastRun.finishDate().compareTo(taskRun.startDate()) < 0, "Task run time conflict found");
                    }
                }
                lastRun = taskRun;
            }
            Preconditions.checkNotNull(lastRun, "No run found for task " + id);
            Preconditions.checkArgument(lastRun.status() != FAILED);
            Preconditions.checkArgument(lastRun.status() != CANCELED);
        }
    }

    public DagRunStatus dagRunDependsStatus() {
        TaskRunStatus taskRunStatus = taskRunDependsStatus(dag.tasks());
        switch (taskRunStatus) {
            case PENDING:
                return DagRunStatus.PENDING;
            case RUNNING:
                return DagRunStatus.PENDING;
            case SUCCESS:
                return DagRunStatus.SUCCESS;
            default:
                throw new IllegalStateException("Illegal task run state: " + taskRunStatus);
        }
    }

    public TaskRunStatus taskRunDependsStatus(Task task) {
        ImmutableList<Task> depends = buildDepends(task);
        return taskRunDependsStatus(depends);
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
        if (hasPending) {
            return PENDING;
        }
        if (hasRunning) {
            return RUNNING;
        }

        return TaskRunStatus.SUCCESS;
    }

    public TaskRun taskRun(Task task) {
        ImmutableList<TaskRun> taskRuns = idTaskRunMap.get(task.id());
        return taskRuns.get(taskRuns.size() - 1);
    }

    private ImmutableList<Task> buildDepends(Task task) {
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

    private ImmutableMap<Long, Task> buildIdTaskMap() {
        ImmutableMap.Builder<Long, Task> builder = ImmutableMap.builder();
        for (Task task : dag.tasks()) {
            builder.put(task.id(), task);
        }
        return builder.build();
    }

    private ImmutableListMultimap<Long, TaskRun> buildIdTaskRunMap() {
        ImmutableListMultimap.Builder<Long, TaskRun> unsortedBuilder = ImmutableListMultimap.builder();
        for (TaskRun task : dagRun.tasks()) {
            unsortedBuilder.put(task.taskId(), task);
        }
        ImmutableListMultimap<Long, TaskRun> unsorted = unsortedBuilder.build();
        ImmutableListMultimap.Builder<Long, TaskRun> sortedBuilder = ImmutableListMultimap.builder();
        for (Long id : unsorted.keySet()) {
            ArrayList<TaskRun> list = new ArrayList<>(unsorted.get(id));
            // reverse order
            list.sort((l, r) -> Long.compare(r.taskId(), l.taskId()));
            sortedBuilder.putAll(id, list);
        }
        return sortedBuilder.build();
    }

    private ImmutableMap<String, Task> buildTokenTaskMap() {
        ImmutableMap.Builder<String, Task> builder = ImmutableMap.builder();
        for (Task task : dag.tasks()) {
            builder.put(task.token(), task);
        }
        return builder.build();
    }
}
