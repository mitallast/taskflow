package org.github.mitallast.taskflow.scheduler;

import com.cronutils.model.Cron;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.github.mitallast.taskflow.common.component.AbstractComponent;

import java.time.Duration;
import java.time.ZonedDateTime;

import static com.cronutils.model.CronType.CRON4J;

public class SchedulerService extends AbstractComponent {

    private final CronDefinition cronDefinition;
    private final CronParser parser;

    @Inject
    public SchedulerService(Config config) {
        super(config, SchedulerService.class);

        cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(CRON4J);
        parser = new CronParser(cronDefinition);
    }

    public boolean validate(String expression) {
        try {
            parser.parse(expression);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    public Duration scheduleNext(String expression) {
        Cron cron = parser.parse(expression);
        ExecutionTime executionTime = ExecutionTime.forCron(cron);
        return executionTime.timeToNextExecution(ZonedDateTime.now());
    }
}
