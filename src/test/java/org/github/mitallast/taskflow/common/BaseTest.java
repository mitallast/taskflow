package org.github.mitallast.taskflow.common;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseTest {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    protected void printQps(String metric, long total, long start, long end) {
        long qps = (long) (total / (double) (end - start) * 1000.);
        logger.info(metric + ": " + total + " at " + (end - start) + "ms");
        logger.info(metric + ": " + qps + " qps");
    }
}
