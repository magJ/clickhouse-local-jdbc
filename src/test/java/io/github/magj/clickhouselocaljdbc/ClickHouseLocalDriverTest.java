package io.github.magj.clickhouselocaljdbc;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class ClickHouseLocalDriverTest {

    @Test
    void acceptsValidUrl() throws Exception {
        ClickHouseLocalDriver driver = new ClickHouseLocalDriver();
        assertTrue(driver.acceptsURL("jdbc:clickhouse-local:"));
        assertTrue(driver.acceptsURL("jdbc:clickhouse-local://anything"));
        assertTrue(driver.acceptsURL("jdbc:clickhouse-local:some/path"));
    }

    @Test
    void rejectsInvalidUrl() throws Exception {
        ClickHouseLocalDriver driver = new ClickHouseLocalDriver();
        assertFalse(driver.acceptsURL("jdbc:mysql://localhost/db"));
        assertFalse(driver.acceptsURL("jdbc:clickhouse://localhost"));
        assertFalse(driver.acceptsURL(null));
        assertFalse(driver.acceptsURL(""));
    }

    @Test
    void connectReturnsConnectionForValidUrl() throws Exception {
        ClickHouseLocalDriver driver = new ClickHouseLocalDriver();
        Properties props = new Properties();
        Connection conn = driver.connect("jdbc:clickhouse-local:", props);
        assertNotNull(conn);
        assertFalse(conn.isClosed());
        conn.close();
        assertTrue(conn.isClosed());
    }

    @Test
    void connectReturnsNullForInvalidUrl() throws Exception {
        ClickHouseLocalDriver driver = new ClickHouseLocalDriver();
        assertNull(driver.connect("jdbc:mysql://localhost/db", new Properties()));
    }

    @Test
    void connectUsesDefaultClickhouseLocalPath() throws Exception {
        ClickHouseLocalDriver driver = new ClickHouseLocalDriver();
        Properties props = new Properties();
        ClickHouseLocalConnection conn = (ClickHouseLocalConnection) driver.connect("jdbc:clickhouse-local:", props);
        assertNotNull(conn);
        assertEquals("clickhouse-local", conn.getClickhouseLocalPath());
        assertNull(conn.getWorkingDirectory());
        conn.close();
    }

    @Test
    void connectParsesCustomProperties() throws Exception {
        ClickHouseLocalDriver driver = new ClickHouseLocalDriver();
        Properties props = new Properties();
        props.setProperty("clickhouseLocalPath", "/usr/local/bin/clickhouse-local");
        props.setProperty("workingDirectory", "/tmp/data");
        ClickHouseLocalConnection conn = (ClickHouseLocalConnection) driver.connect("jdbc:clickhouse-local:", props);
        assertNotNull(conn);
        assertEquals("/usr/local/bin/clickhouse-local", conn.getClickhouseLocalPath());
        assertEquals("/tmp/data", conn.getWorkingDirectory());
        conn.close();
    }

    @Test
    void driverVersionInfo() throws Exception {
        ClickHouseLocalDriver driver = new ClickHouseLocalDriver();
        assertEquals(1, driver.getMajorVersion());
        assertEquals(0, driver.getMinorVersion());
        assertFalse(driver.jdbcCompliant());
    }

    @Test
    void driverRegisteredWithDriverManager() throws Exception {
        // Driver should be auto-registered via static block
        Connection conn = DriverManager.getConnection("jdbc:clickhouse-local:");
        assertNotNull(conn);
        conn.close();
    }
}
