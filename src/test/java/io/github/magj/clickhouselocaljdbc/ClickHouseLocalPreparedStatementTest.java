package io.github.magj.clickhouselocaljdbc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ClickHouseLocalPreparedStatementTest {

    private ClickHouseLocalConnection connection;

    @BeforeEach
    void setUp() {
        connection = new ClickHouseLocalConnection("clickhouse-local", null);
    }

    private ClickHouseLocalPreparedStatement prepare(String sql) {
        return new ClickHouseLocalPreparedStatement(connection, sql);
    }

    @Test
    void buildSqlWithStringParameter() throws SQLException {
        var ps = prepare("SELECT * FROM t WHERE name = ?");
        ps.setString(1, "Alice");
        assertEquals("SELECT * FROM t WHERE name = {p1:String}", ps.buildSql());
        assertEquals(List.of("--param_p1=Alice"), ps.buildParamArgs());
    }

    @Test
    void buildSqlWithIntParameter() throws SQLException {
        var ps = prepare("SELECT * FROM t WHERE id = ?");
        ps.setInt(1, 42);
        assertEquals("SELECT * FROM t WHERE id = 42", ps.buildSql());
    }

    @Test
    void buildSqlWithLongParameter() throws SQLException {
        var ps = prepare("SELECT ? + ?");
        ps.setLong(1, 100L);
        ps.setLong(2, 200L);
        assertEquals("SELECT 100 + 200", ps.buildSql());
    }

    @Test
    void buildSqlWithDoubleParameter() throws SQLException {
        var ps = prepare("SELECT ?");
        ps.setDouble(1, 3.14);
        assertEquals("SELECT 3.14", ps.buildSql());
    }

    @Test
    void buildSqlWithBooleanParameter() throws SQLException {
        var ps = prepare("SELECT ?");
        ps.setBoolean(1, true);
        assertEquals("SELECT 1", ps.buildSql());
        ps.clearParameters();
        ps.setBoolean(1, false);
        assertEquals("SELECT 0", ps.buildSql());
    }

    @Test
    void buildSqlWithNullParameter() throws SQLException {
        var ps = prepare("SELECT ?");
        ps.setNull(1, java.sql.Types.INTEGER);
        assertEquals("SELECT NULL", ps.buildSql());
    }

    @Test
    void buildSqlWithNullString() throws SQLException {
        var ps = prepare("SELECT ?");
        ps.setString(1, null);
        assertEquals("SELECT NULL", ps.buildSql());
        assertEquals(List.of(), ps.buildParamArgs());
    }

    @Test
    void buildSqlEscapesSingleQuoteInString() throws SQLException {
        var ps = prepare("SELECT ?");
        ps.setString(1, "it's");
        assertEquals("SELECT {p1:String}", ps.buildSql());
        assertEquals(List.of("--param_p1=it's"), ps.buildParamArgs());
    }

    @Test
    void buildSqlEscapesBackslashInString() throws SQLException {
        var ps = prepare("SELECT ?");
        ps.setString(1, "back\\slash");
        assertEquals("SELECT {p1:String}", ps.buildSql());
        assertEquals(List.of("--param_p1=back\\slash"), ps.buildParamArgs());
    }

    @Test
    void buildSqlWithMultipleParameters() throws SQLException {
        var ps = prepare("INSERT INTO t (a, b, c) VALUES (?, ?, ?)");
        ps.setInt(1, 1);
        ps.setString(2, "hello");
        ps.setDouble(3, 1.5);
        assertEquals("INSERT INTO t (a, b, c) VALUES (1, {p2:String}, 1.5)", ps.buildSql());
        assertEquals(List.of("--param_p2=hello"), ps.buildParamArgs());
    }

    @Test
    void buildSqlIgnoresQuestionMarkInSingleQuotes() throws SQLException {
        var ps = prepare("SELECT '?' AS literal, ?");
        ps.setInt(1, 99);
        assertEquals("SELECT '?' AS literal, 99", ps.buildSql());
    }

    @Test
    void buildSqlIgnoresQuestionMarkInDoubleQuotes() throws SQLException {
        var ps = prepare("SELECT \"?\" AS literal, ?");
        ps.setInt(1, 77);
        assertEquals("SELECT \"?\" AS literal, 77", ps.buildSql());
    }

    @Test
    void clearParametersRemovesAllParams() throws SQLException {
        var ps = prepare("SELECT ?, ?");
        ps.setInt(1, 1);
        ps.setString(2, "hello");
        ps.clearParameters();
        assertThrows(SQLException.class, ps::buildSql);
        assertEquals(List.of(), ps.buildParamArgs());
    }

    @Test
    void buildSqlWithBigDecimalParameter() throws SQLException {
        var ps = prepare("SELECT ?");
        ps.setBigDecimal(1, new BigDecimal("123.456"));
        assertEquals("SELECT 123.456", ps.buildSql());
    }

    @Test
    void buildSqlWithObjectString() throws SQLException {
        var ps = prepare("SELECT ?");
        ps.setObject(1, "world");
        assertEquals("SELECT {p1:String}", ps.buildSql());
        assertEquals(List.of("--param_p1=world"), ps.buildParamArgs());
    }

    @Test
    void buildSqlWithObjectInteger() throws SQLException {
        var ps = prepare("SELECT ?");
        ps.setObject(1, 42);
        assertEquals("SELECT 42", ps.buildSql());
    }

    @Test
    void buildSqlWithObjectNull() throws SQLException {
        var ps = prepare("SELECT ?");
        ps.setObject(1, null);
        assertEquals("SELECT NULL", ps.buildSql());
    }

    @Test
    void buildSqlWithDateParameter() throws SQLException {
        var ps = prepare("SELECT ?");
        ps.setDate(1, Date.valueOf("2024-01-15"));
        assertEquals("SELECT '2024-01-15'", ps.buildSql());
    }

    @Test
    void buildSqlThrowsWhenParamNotSet() throws SQLException {
        var ps = prepare("SELECT ?");
        assertThrows(SQLException.class, ps::buildSql);
    }
}
