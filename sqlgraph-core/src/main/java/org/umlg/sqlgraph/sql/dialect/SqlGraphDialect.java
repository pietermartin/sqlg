package org.umlg.sqlgraph.sql.dialect;

public enum SqlGraphDialect {
    POSTGRES("org.postgresql.xa.PGXADataSource", new PostgresDialect()),
    HSQLDB("org.hsqldb.jdbc.JDBCDriver", new HsqldbDialect()),
    MARIADBDB("org.mariadb.jdbc.Driver", new MariaDbDialect()),
    DERBYDB("org.apache.derby.jdbc.EmbeddedDriver", new DerbyDialect());
    private String jdbcDriver;
    private SqlDialect sqlDialect;

    private SqlGraphDialect(String jdbcDriver, SqlDialect sqlDialect) {
        this.jdbcDriver = jdbcDriver;
        this.sqlDialect = sqlDialect;
    }

    public String getJdbcDriver() {
        return jdbcDriver;
    }

    public SqlDialect getSqlDialect() {
        return sqlDialect;
    }
}
