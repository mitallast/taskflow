package org.github.mitallast.taskflow.persistence;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.github.mitallast.taskflow.common.component.AbstractLifecycleComponent;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.JDBCUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PersistenceService extends AbstractLifecycleComponent {

    private final String url;
    private final String username;
    private final String password;
    private final SQLDialect dialect;

    private final Connection connection;

    @Inject
    public PersistenceService(Config config) throws SQLException {
        super(config.getConfig("persistence"), PersistenceService.class);
        url = this.config.getString("url");
        username = this.config.getString("username");
        password = this.config.getString("password");

        connection = DriverManager.getConnection(
            url,
            username,
            password
        );
        dialect = JDBCUtils.dialect(url);
    }

    public DSLContext context() {
        return DSL.using(connection, dialect);
    }

    @Override
    protected void doStart() throws IOException {
    }

    @Override
    protected void doStop() throws IOException {

    }

    @Override
    protected void doClose() throws IOException {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
}
