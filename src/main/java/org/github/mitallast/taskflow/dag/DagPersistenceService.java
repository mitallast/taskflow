package org.github.mitallast.taskflow.dag;

import com.google.common.collect.ImmutableList;
import org.github.mitallast.taskflow.common.Immutable;

import java.util.Collection;
import java.util.Optional;

public interface DagPersistenceService {

    Dag createDag(Dag dag);

    Dag updateDag(Dag dag);

    ImmutableList<Dag> findLatestDags();

    Optional<Dag> findDagById(long id);

    ImmutableList<Dag> findDagByIds(Collection<Long> ids);

    Optional<Dag> findDagByToken(String token);
}
