package io.github.magj.clickhouselocaljdbc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

class ClickHouseLocalStatementTest {

    private ClickHouseLocalConnection connection;
    private ClickHouseLocalStatement statement;

    @BeforeEach
    void setUp() {
        connection = new ClickHouseLocalConnection("clickhouse-local", null);
        statement = new ClickHouseLocalStatement(connection);
    }

    @Test
    void parseTabSeparatedOutputWithRows() throws SQLException {
        String output = "id\tname\tvalue\n" +
                        "Int32\tString\tFloat64\n" +
                        "1\tAlice\t9.5\n" +
                        "2\tBob\t7.2\n";
        ClickHouseLocalResultSet rs = statement.parseTabSeparatedOutput(output);
        assertNotNull(rs);
        assertEquals(3, rs.getMetaData().getColumnCount());
        assertEquals("id", rs.getMetaData().getColumnName(1));
        assertEquals("name", rs.getMetaData().getColumnName(2));
        assertEquals("value", rs.getMetaData().getColumnName(3));

        assertTrue(rs.next());
        assertEquals("1", rs.getString(1));
        assertEquals("Alice", rs.getString(2));
        assertEquals(9.5, rs.getDouble(3), 0.001);

        assertTrue(rs.next());
        assertEquals("2", rs.getString(1));
        assertEquals("Bob", rs.getString(2));
        assertEquals(7.2, rs.getDouble(3), 0.001);

        assertFalse(rs.next());
    }

    @Test
    void parseTabSeparatedOutputEmpty() {
        String output = "";
        ClickHouseLocalResultSet rs = statement.parseTabSeparatedOutput(output);
        assertNotNull(rs);
        assertEquals(0, rs.getColumnCount());
    }

    @Test
    void parseTabSeparatedOutputHeaderOnly() throws SQLException {
        String output = "id\tname\n" +
                        "Int32\tString\n";
        ClickHouseLocalResultSet rs = statement.parseTabSeparatedOutput(output);
        assertNotNull(rs);
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertFalse(rs.next());
    }

    @Test
    void parseTabSeparatedOutputWithNullValues() throws SQLException {
        String output = "id\tname\n" +
                        "Int32\tNullable(String)\n" +
                        "1\t\\N\n";
        ClickHouseLocalResultSet rs = statement.parseTabSeparatedOutput(output);
        assertTrue(rs.next());
        assertEquals("1", rs.getString(1));
        assertNull(rs.getString(2));
        assertTrue(rs.wasNull());
    }

    @Test
    void parseTabSeparatedOutputSingleColumn() throws SQLException {
        String output = "result\nInt32\n42\n";
        ClickHouseLocalResultSet rs = statement.parseTabSeparatedOutput(output);
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertTrue(rs.next());
        assertEquals(42, rs.getInt(1));
        assertFalse(rs.next());
    }

    @Test
    void closeStatement() throws SQLException {
        assertFalse(statement.isClosed());
        statement.close();
        assertTrue(statement.isClosed());
    }

    @Test
    void executeQueryOnClosedStatementThrows() throws SQLException {
        statement.close();
        assertThrows(SQLException.class, () -> statement.executeQuery("SELECT 1"));
    }

    @Test
    void getConnectionReturnsConnection() throws SQLException {
        assertEquals(connection, statement.getConnection());
    }

    @Test
    void initialResultSetAndUpdateCountAreNull() throws SQLException {
        assertNull(statement.getResultSet());
        assertEquals(-1, statement.getUpdateCount());
    }

    @Test
    void parseTabSeparatedOutputWithTrailingNewline() throws SQLException {
        String output = "x\nInt32\n1\n2\n3\n";
        ClickHouseLocalResultSet rs = statement.parseTabSeparatedOutput(output);
        int count = 0;
        while (rs.next()) count++;
        assertEquals(3, count);
    }

    @Test
    void parseTabSeparatedOutputUnescapesTabsAndNewlines() throws SQLException {
        // ClickHouse TSV escapes embedded tabs as \t and newlines as \n within field values
        String output = "id\ttext\n" +
                        "Int32\tString\n" +
                        "1\thello\\tworld\n" +
                        "2\thello\\nworld\n";
        ClickHouseLocalResultSet rs = statement.parseTabSeparatedOutput(output);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("hello\tworld", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("hello\nworld", rs.getString(2));
        assertFalse(rs.next());
    }

    @Test
    void parseTabSeparatedOutputUnescapesSingleQuote() throws SQLException {
        // ClickHouse TSV escapes single quotes as \' within String field values
        String output = "val\nString\nit\\'s a test\n";
        ClickHouseLocalResultSet rs = statement.parseTabSeparatedOutput(output);
        assertTrue(rs.next());
        assertEquals("it's a test", rs.getString(1));
    }
}
