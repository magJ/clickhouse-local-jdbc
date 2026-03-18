package io.github.magj.clickhouselocaljdbc;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class ClickHouseLocalIntegrationTest {

    private static String clickhouseLocalPath;

    @BeforeAll
    static void setUp() {
        clickhouseLocalPath = System.getProperty("clickhouseLocalPath", "clickhouse-local");
    }

    private Connection connect() throws SQLException {
        Properties props = new Properties();
        props.setProperty("clickhouseLocalPath", clickhouseLocalPath);
        return DriverManager.getConnection("jdbc:clickhouse-local:", props);
    }

    @Test
    void selectConstant() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 42")) {
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void selectMultipleColumns() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT toInt32(1) AS id, 'hello' AS name")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("hello", rs.getString("name"));
            assertFalse(rs.next());
        }
    }

    @Test
    void selectMultipleRows() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT number FROM system.numbers LIMIT 5")) {
            for (long i = 0; i < 5; i++) {
                assertTrue(rs.next());
                assertEquals(i, rs.getLong(1));
            }
            assertFalse(rs.next());
        }
    }

    @Test
    void selectNullValue() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT NULL")) {
            assertTrue(rs.next());
            assertNull(rs.getString(1));
            assertTrue(rs.wasNull());
        }
    }

    @Test
    void selectStringWithEmbeddedTab() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT concat('hello', char(9), 'world') AS s")) {
            assertTrue(rs.next());
            assertEquals("hello\tworld", rs.getString("s"));
        }
    }

    @Test
    void selectStringWithEmbeddedNewline() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT concat('hello', char(10), 'world') AS s")) {
            assertTrue(rs.next());
            assertEquals("hello\nworld", rs.getString("s"));
        }
    }

    @Test
    void selectStringWithEmbeddedBackslash() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 'C:\\\\Users' AS path")) {
            assertTrue(rs.next());
            assertEquals("C:\\Users", rs.getString("path"));
        }
    }

    @Test
    void selectMultipleColumnsWithTabsAndNewlines() throws SQLException {
        // Verifies TSV parsing: escaped \t in a column value must not shift subsequent
        // column values, and escaped \n in a column value must not split the row.
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT toInt32(1) AS id, " +
                     "concat('col1', char(9), 'col2') AS with_tab, " +
                     "concat('line1', char(10), 'line2') AS with_newline")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("col1\tcol2", rs.getString("with_tab"));
            assertEquals("line1\nline2", rs.getString("with_newline"));
            assertFalse(rs.next());
        }
    }

    @Test
    void selectNumericTypes() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT toInt8(127) AS tiny, toInt32(2147483647) AS i, toInt64(9000000000) AS big, toFloat64(3.14) AS d")) {
            assertTrue(rs.next());
            assertEquals(127, rs.getByte("tiny"));
            assertEquals(2147483647, rs.getInt("i"));
            assertEquals(9000000000L, rs.getLong("big"));
            assertEquals(3.14, rs.getDouble("d"), 0.001);
        }
    }

    @Test
    void selectNullableColumn() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT toNullable(toInt32(1)) AS present, toNullable(toInt32(NULL)) AS absent")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("present"));
            assertFalse(rs.wasNull());
            assertEquals(0, rs.getInt("absent"));
            assertTrue(rs.wasNull());
        }
    }

    @Test
    void selectWithPreparedStatement() throws SQLException {
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement("SELECT ? AS val")) {
            ps.setString(1, "hello world");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("hello world", rs.getString("val"));
            }
        }
    }

    @Test
    void selectWithPreparedStatementSpecialChars() throws SQLException {
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement("SELECT ? AS val")) {
            ps.setString(1, "it's a test");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("it's a test", rs.getString("val"));
            }
        }
    }

    @Test
    void resultSetMetaData() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT toInt32(1) AS id, 'hello' AS name")) {
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals(2, meta.getColumnCount());
            assertEquals("id", meta.getColumnName(1));
            assertEquals("name", meta.getColumnName(2));
            assertEquals(Types.INTEGER, meta.getColumnType(1));
            assertEquals(Types.VARCHAR, meta.getColumnType(2));
        }
    }

    @Test
    void databaseProductVersion() throws SQLException {
        try (Connection conn = connect()) {
            String version = conn.getMetaData().getDatabaseProductVersion();
            assertNotNull(version);
            assertFalse(version.isEmpty());
            assertNotEquals("unknown", version);
        }
    }
}
