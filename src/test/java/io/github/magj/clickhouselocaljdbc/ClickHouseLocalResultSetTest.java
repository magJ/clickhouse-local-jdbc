package io.github.magj.clickhouselocaljdbc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ClickHouseLocalResultSetTest {

    private ClickHouseLocalResultSet rs;

    @BeforeEach
    void setUp() {
        List<String> cols = Arrays.asList("id", "name", "score", "active");
        List<String> types = Arrays.asList("Int32", "String", "Float64", "UInt8");
        List<String[]> rows = Arrays.asList(
            new String[]{"1", "Alice", "9.5", "1"},
            new String[]{"2", "Bob", "7.2", "0"},
            new String[]{"3", "\\N", "\\N", "1"}
        );
        rs = new ClickHouseLocalResultSet(cols, types, rows);
    }

    @Test
    void nextAdvancesCursor() throws SQLException {
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertFalse(rs.next());
    }

    @Test
    void getRowReturnsOneBased() throws SQLException {
        assertEquals(0, rs.getRow());
        rs.next();
        assertEquals(1, rs.getRow());
        rs.next();
        assertEquals(2, rs.getRow());
    }

    @Test
    void getStringByIndex() throws SQLException {
        rs.next();
        assertEquals("1", rs.getString(1));
        assertEquals("Alice", rs.getString(2));
    }

    @Test
    void getStringByColumnName() throws SQLException {
        rs.next();
        assertEquals("Alice", rs.getString("name"));
        assertEquals("1", rs.getString("id"));
    }

    @Test
    void getIntValue() throws SQLException {
        rs.next();
        assertEquals(1, rs.getInt(1));
        rs.next();
        assertEquals(2, rs.getInt(1));
    }

    @Test
    void getDoubleValue() throws SQLException {
        rs.next();
        assertEquals(9.5, rs.getDouble(3), 0.001);
        rs.next();
        assertEquals(7.2, rs.getDouble(3), 0.001);
    }

    @Test
    void getBooleanFromOne() throws SQLException {
        rs.next();
        assertTrue(rs.getBoolean(4));
        rs.next();
        assertFalse(rs.getBoolean(4));
    }

    @Test
    void getBooleanFromTrueString() throws SQLException {
        List<String> cols = Collections.singletonList("flag");
        List<String> types = Collections.singletonList("String");
        List<String[]> rows = Arrays.asList(
            new String[]{"true"},
            new String[]{"false"},
            new String[]{"TRUE"}
        );
        ClickHouseLocalResultSet boolRs = new ClickHouseLocalResultSet(cols, types, rows);
        boolRs.next();
        assertTrue(boolRs.getBoolean(1));
        boolRs.next();
        assertFalse(boolRs.getBoolean(1));
        boolRs.next();
        assertTrue(boolRs.getBoolean(1));
    }

    @Test
    void nullValueReturnsNullAndWasNull() throws SQLException {
        rs.next();
        rs.next();
        rs.next(); // row with \N
        assertNull(rs.getString(2));
        assertTrue(rs.wasNull());
        assertEquals(0.0, rs.getDouble(3), 0.0);
        assertTrue(rs.wasNull());
    }

    @Test
    void wasNullFalseAfterNonNullGet() throws SQLException {
        rs.next();
        rs.getString(1);
        assertFalse(rs.wasNull());
    }

    @Test
    void getBigDecimal() throws SQLException {
        rs.next();
        BigDecimal bd = rs.getBigDecimal(3);
        assertNotNull(bd);
        assertEquals(0, new BigDecimal("9.5").compareTo(bd));
    }

    @Test
    void findColumnByName() throws SQLException {
        assertEquals(1, rs.findColumn("id"));
        assertEquals(2, rs.findColumn("name"));
        assertEquals(3, rs.findColumn("score"));
        assertEquals(4, rs.findColumn("active"));
    }

    @Test
    void findColumnThrowsForUnknownColumn() {
        assertThrows(SQLException.class, () -> rs.findColumn("unknown"));
    }

    @Test
    void closeAndIsClosed() throws SQLException {
        assertFalse(rs.isClosed());
        rs.close();
        assertTrue(rs.isClosed());
    }

    @Test
    void getMetaDataReturnsValidMetaData() throws SQLException {
        assertNotNull(rs.getMetaData());
        assertEquals(4, rs.getMetaData().getColumnCount());
    }

    @Test
    void emptyResultSet() throws SQLException {
        ClickHouseLocalResultSet empty = new ClickHouseLocalResultSet(
            Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        assertFalse(empty.next());
        assertEquals(0, empty.getColumnCount());
    }

    @Test
    void getColumnCount() {
        assertEquals(4, rs.getColumnCount());
    }

    @Test
    void getLongValue() throws SQLException {
        rs.next();
        assertEquals(1L, rs.getLong(1));
    }

    @Test
    void getFloatValue() throws SQLException {
        rs.next();
        assertEquals(9.5f, rs.getFloat(3), 0.01f);
    }

    @Test
    void getShortValue() throws SQLException {
        rs.next();
        assertEquals((short) 1, rs.getShort(1));
    }

    @Test
    void getByteValue() throws SQLException {
        rs.next();
        assertEquals((byte) 1, rs.getByte(1));
    }

    @Test
    void getObjectForInt32() throws SQLException {
        rs.next();
        Object obj = rs.getObject(1);
        assertNotNull(obj);
        assertEquals(Integer.class, obj.getClass());
        assertEquals(1, obj);
    }

    @Test
    void getObjectForFloat64() throws SQLException {
        rs.next();
        Object obj = rs.getObject(3);
        assertNotNull(obj);
        assertEquals(Double.class, obj.getClass());
    }
}
