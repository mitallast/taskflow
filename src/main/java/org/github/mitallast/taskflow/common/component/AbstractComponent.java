package org.github.mitallast.taskflow.common.component;

import com.typesafe.config.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractComponent {
    protected final Logger logger;
    protected Config config;

    public AbstractComponent(Config config, Class loggerClass) {
        this.config = config;
        this.logger = LogManager.getLogger(loggerClass);
    }
}