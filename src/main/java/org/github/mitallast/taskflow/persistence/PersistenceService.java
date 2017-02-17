package org.github.mitallast.taskflow.persistence;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariPool;
import org.github.mitallast.taskflow.common.component.AbstractLifecycleComponent;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.JDBCUtils;

import java.io.IOException;
import java.sql.Connection;
import java.util.Map;
import java.util.Properties;

public class PersistenceService extends AbstractLifecycleComponent {

    private final String url;
    private final String username;
    private final String password;
    private final long await;
    private final SQLDialect dialect;
    private final Properties properties;
    private final HikariConfig conf;
    private final HikariDataSource dataSource;

    @Inject
    public PersistenceService(Config config) throws Exception {
        super(config.getConfig("persistence"), PersistenceService.class);
        url = this.config.getString("url");
        username = this.config.getIsNull("username") ? null : this.config.getString("username");
        password = this.config.getIsNull("password") ? null : this.config.getString("password");
        await = this.config.getDuration("await").toMillis();

        dialect = JDBCUtils.dialect(url);

        properties = new Properties();
        for (Map.Entry<String, ConfigValue> entry : this.config.getConfig("properties").entrySet()) {
            properties.put(entry.getKey(), entry.getValue().unwrapped());
        }

        conf = new HikariConfig(properties);
        conf.setJdbcUrl(url);
        conf.setUsername(username);
        conf.setPassword(password);
        conf.setInitializationFailFast(false);

        dataSource = new HikariDataSource(conf);

        await();
    }

    private void await() throws Exception {
        long start = System.currentTimeMillis();
        long timeout = start + await;
        Throwable lastError = null;
        logger.info("check database connection");
        while (System.currentTimeMillis() < timeout) {
            try (Connection connection = dataSource.getConnection()) {
                if (!connection.getAutoCommit()) {
                    connection.commit();
                }
                logger.info("successful connect to database");
                break;
            } catch (Throwable e) {
                logger.warn("failed retrieve connection, await");
                lastError = e;
                Thread.sleep(1000);
            }
        }
        if (lastError != null) {
            throw new HikariPool.PoolInitializationException(lastError);
        }
    }

    public DSLContext context() {
        return DSL.using(dataSource, dialect);
    }

    @Override
    protected void doStart() throws IOException {
    }

    @Override
    protected void doStop() throws IOException {

    }

    @Override
    protected void doClose() throws IOException {
        dataSource.close();
    }
}
