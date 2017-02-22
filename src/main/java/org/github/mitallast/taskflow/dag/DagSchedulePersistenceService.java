package org.github.mitallast.taskflow.dag;

import com.google.common.collect.ImmutableList;

import java.util.Optional;

public interface DagSchedulePersistenceService {

    ImmutableList<DagSchedule> findDagSchedules();

    ImmutableList<DagSchedule> findEnabledDagSchedules();

    Optional<DagSchedule> findDagSchedule(String token);

    boolean markDagScheduleEnabled(String token);

    boolean markDagScheduleDisabled(String token);

    boolean updateSchedule(DagSchedule dagSchedule);
}
