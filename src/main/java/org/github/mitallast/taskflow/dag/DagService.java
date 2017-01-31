package org.github.mitallast.taskflow.dag;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.github.mitallast.taskflow.common.component.AbstractComponent;

public class DagService extends AbstractComponent {

    @Inject
    public DagService(Config config) {
        super(config, DagService.class);
    }
}
