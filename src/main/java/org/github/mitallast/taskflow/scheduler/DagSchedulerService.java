package org.github.mitallast.taskflow.scheduler;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.github.mitallast.taskflow.common.component.AbstractLifecycleComponent;
import org.github.mitallast.taskflow.common.error.Errors;
import org.github.mitallast.taskflow.common.error.MaybeErrors;
import org.github.mitallast.taskflow.dag.Dag;
import org.github.mitallast.taskflow.dag.DagPersistenceService;
import org.github.mitallast.taskflow.dag.DagSchedule;
import org.github.mitallast.taskflow.dag.DagService;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class DagSchedulerService extends AbstractLifecycleComponent {

    private final SchedulerService schedulerService;
    private final ScheduledExecutorService executorService;
    private final DagPersistenceService persistenceService;
    private final DagService dagService;

    private volatile ScheduledFuture task;

    @Inject
    public DagSchedulerService(
        Config config,
        SchedulerService schedulerService,
        DagPersistenceService persistenceService,
        DagService dagService
    ) {
        super(config, DagSchedulerService.class);
        this.schedulerService = schedulerService;
        this.persistenceService = persistenceService;
        this.dagService = dagService;

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
        if (persistenceService.markDagScheduleEnabled(token)) {
            schedule();
            return false;
        } else {
            return false;
        }
    }

    public boolean markDagScheduleDisabled(String token) {
        logger.info("disable {}", token);
        if (persistenceService.markDagScheduleDisabled(token)) {
            schedule();
            return false;
        } else {
            return false;
        }
    }

    public MaybeErrors<Boolean> update(DagSchedule dagSchedule) {
        logger.info("update {}", dagSchedule);
        return validate(dagSchedule).maybe(() -> {
            if (persistenceService.updateSchedule(dagSchedule)) {
                schedule();
                return true;
            } else {
                return false;
            }
        });
    }

    private void schedule() {
        executorService.execute(this::doSchedule);
    }

    private void doSchedule() {
        logger.info("schedule dag run");
        try {
            DagSchedule schedule = null;
            Duration scheduleDuration = null;

            for (DagSchedule item : persistenceService.findEnabledDagSchedules()) {
                Duration itemDuration = schedulerService.scheduleNext(item.cronExpression());
                if (schedule == null || scheduleDuration.toMillis() > itemDuration.toMillis()) {
                    schedule = item;
                    scheduleDuration = itemDuration;
                }
            }

            if (schedule != null && scheduleDuration != null) {
                doSchedule(schedule, scheduleDuration);
            }
        } catch (Exception e) {
            logger.warn("error schedule dag run", e);
        }
    }

    private void doSchedule(DagSchedule schedule, Duration scheduleDuration) {
        logger.info("schedule dag {} in {}", schedule.token(), scheduleDuration);
        if (task != null) {
            task.cancel(false);
            task = null;
        }
        task = executorService.schedule(() -> {
            task = null;
            Optional<Dag> dag = persistenceService.findDagByToken(schedule.token());
            if (dag.isPresent()) {
                dagService.createDagRun(dag.get());
            } else {
                logger.warn("dag {} not found", schedule.token());
            }
            schedule();
        }, scheduleDuration.toMillis(), TimeUnit.MILLISECONDS);
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
