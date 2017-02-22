package org.github.mitallast.taskflow.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.github.mitallast.taskflow.common.component.AbstractLifecycleComponent;
import org.github.mitallast.taskflow.common.error.Errors;
import org.github.mitallast.taskflow.common.error.MaybeErrors;
import org.github.mitallast.taskflow.dag.*;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.*;

public class DagSchedulerService extends AbstractLifecycleComponent {

    private final SchedulerService schedulerService;
    private final ScheduledExecutorService executorService;
    private final DagPersistenceService dagPersistence;
    private final DagRunPersistenceService dagRunPersistence;
    private final DagSchedulePersistenceService dagSchedulePersistence;
    private final DagService dagService;

    private final ConcurrentMap<String, ScheduledFuture> tasks;

    @Inject
    public DagSchedulerService(
        Config config,
        SchedulerService schedulerService,
        DagPersistenceService dagPersistence,
        DagRunPersistenceService dagRunPersistence,
        DagSchedulePersistenceService dagSchedulePersistence,
        DagService dagService
    ) {
        super(config, DagSchedulerService.class);
        this.schedulerService = schedulerService;
        this.dagPersistence = dagPersistence;
        this.dagRunPersistence = dagRunPersistence;
        this.dagSchedulePersistence = dagSchedulePersistence;
        this.dagService = dagService;

        this.tasks = new ConcurrentHashMap<>();
        this.executorService = Executors.newSingleThreadScheduledExecutor();
    }

    public Errors validate(DagSchedule dagSchedule) {
        logger.info("validate dag schedule: {}", dagSchedule);
        Errors builder = new Errors();
        builder.required(dagSchedule.token()).accept("token", "required");
        builder.required(dagSchedule.cronExpression()).accept("cronExpression", "required");
        builder.not(schedulerService.validate(dagSchedule.cronExpression())).accept("cronExpression", "invalid format");

        if (!builder.valid()) {
            logger.warn("dag schedule errors: {}", builder.toString());
        } else {
            logger.info("dag schedule is valid");
        }

        return builder;
    }

    public boolean markDagScheduleEnabled(String token) {
        logger.info("enable {}", token);
        if (dagSchedulePersistence.markDagScheduleEnabled(token)) {
            schedule();
            return false;
        } else {
            return false;
        }
    }

    public boolean markDagScheduleDisabled(String token) {
        logger.info("disable {}", token);
        if (dagSchedulePersistence.markDagScheduleDisabled(token)) {
            schedule();
            return false;
        } else {
            return false;
        }
    }

    public MaybeErrors<Boolean> update(DagSchedule dagSchedule) {
        logger.info("update {}", dagSchedule);
        return validate(dagSchedule).maybe(() -> {
            if (dagSchedulePersistence.updateSchedule(dagSchedule)) {
                schedule();
                return true;
            } else {
                return false;
            }
        });
    }

    private void schedule() {
        executorService.execute(() -> {
            logger.info("schedule dag run");
            for (DagSchedule item : dagSchedulePersistence.findEnabledDagSchedules()) {
                try {
                    schedule(item);
                } catch (Exception e) {
                    logger.warn("error schedule dag run", e);
                }
            }
        });
    }

    private void schedule(DagSchedule schedule) {
        cancel(schedule.token());
        Duration duration = schedulerService.scheduleNext(schedule.cronExpression());
        logger.info("schedule dag {} in {}", schedule.token(), duration);
        ScheduledFuture future = executorService.schedule(() -> {
            Optional<Dag> dag = dagPersistence.findDagByToken(schedule.token());
            if (dag.isPresent()) {
                ImmutableList<DagRun> pending = dagRunPersistence.findPendingDagRunsByDag(dag.get().id());
                if (pending.isEmpty()) {
                    dagService.createDagRun(dag.get());
                } else {
                    logger.info("pending dag run {} exists", dag.get().token());
                }
            } else {
                logger.warn("dag {} not found", schedule.token());
            }
            schedule(schedule);
        }, duration.toMillis(), TimeUnit.MILLISECONDS);
        tasks.put(schedule.token(), future);
    }

    private void cancel(String token) {
        logger.info("cancel dag run schedule {}", token);
        ScheduledFuture task = tasks.remove(token);
        if (task != null) {
            task.cancel(false);
        }
    }

    @Override
    protected void doStart() throws IOException {
        schedule();
    }

    @Override
    protected void doStop() throws IOException {

    }

    @Override
    protected void doClose() throws IOException {

    }
}
