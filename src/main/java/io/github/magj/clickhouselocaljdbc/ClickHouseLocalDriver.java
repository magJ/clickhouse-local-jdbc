package io.github.magj.clickhouselocaljdbc;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class ClickHouseLocalDriver implements Driver {

    static final String URL_PREFIX = "jdbc:clickhouse-local:";
    static final int DRIVER_MAJOR_VERSION = 1;
    static final int DRIVER_MINOR_VERSION = 0;
    static final String DRIVER_VERSION = DRIVER_MAJOR_VERSION + "." + DRIVER_MINOR_VERSION;

    static {
        try {
            DriverManager.registerDriver(new ClickHouseLocalDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register ClickHouseLocalDriver", e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        Properties props = info != null ? info : new Properties();
        String clickhouseLocalPath = props.getProperty("clickhouseLocalPath", "clickhouse-local");
        String workingDirectory = props.getProperty("workingDirectory", null);
        return new ClickHouseLocalConnection(clickhouseLocalPath, workingDirectory);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[]{
            new DriverPropertyInfo("clickhouseLocalPath", "clickhouse-local"),
            new DriverPropertyInfo("workingDirectory", null)
        };
    }

    @Override
    public int getMajorVersion() {
        return DRIVER_MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return DRIVER_MINOR_VERSION;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("getParentLogger not supported");
    }
}
