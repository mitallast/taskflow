package org.github.mitallast.taskflow.persistence;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.github.mitallast.taskflow.common.component.AbstractLifecycleComponent;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.JDBCUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class PersistenceService extends AbstractLifecycleComponent {

    private final String url;
    private final String username;
    private final String password;
    private final SQLDialect dialect;
    private final Properties properties;
    private final HikariConfig conf;
    private final HikariDataSource dataSource;

    @Inject
    public PersistenceService(Config config) throws SQLException {
        super(config.getConfig("persistence"), PersistenceService.class);
        url = this.config.getString("url");
        username = this.config.getIsNull("username") ? null : this.config.getString("username");
        password = this.config.getIsNull("password") ? null : this.config.getString("password");

        dialect = JDBCUtils.dialect(url);

        properties = new Properties();
        for (Map.Entry<String, ConfigValue> entry : this.config.getConfig("properties").entrySet()) {
            properties.put(entry.getKey(), entry.getValue().unwrapped());
        }

        conf = new HikariConfig(properties);
        conf.setJdbcUrl(url);
        conf.setUsername(username);
        conf.setPassword(password);

        dataSource = new HikariDataSource(conf);
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
