package io.github.magj.clickhouselocaljdbc;

import org.junit.jupiter.api.Test;

import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ClickHouseLocalResultSetMetaDataTest {

    private ClickHouseLocalResultSetMetaData makeMetaData(List<String> names, List<String> types) {
        return new ClickHouseLocalResultSetMetaData(names, types);
    }

    @Test
    void getColumnCount() throws Exception {
        var meta = makeMetaData(Arrays.asList("a", "b", "c"), Arrays.asList("Int32", "String", "Float64"));
        assertEquals(3, meta.getColumnCount());
    }

    @Test
    void getColumnName() throws Exception {
        var meta = makeMetaData(Arrays.asList("id", "name"), Arrays.asList("Int32", "String"));
        assertEquals("id", meta.getColumnName(1));
        assertEquals("name", meta.getColumnName(2));
    }

    @Test
    void getColumnLabel() throws Exception {
        var meta = makeMetaData(Arrays.asList("id"), Arrays.asList("Int32"));
        assertEquals("id", meta.getColumnLabel(1));
    }

    @Test
    void getColumnTypeName() throws Exception {
        var meta = makeMetaData(Arrays.asList("a"), Arrays.asList("DateTime64(3)"));
        assertEquals("DateTime64(3)", meta.getColumnTypeName(1));
    }

    @Test
    void typeMapping_Int8() throws Exception {
        var meta = makeMetaData(Arrays.asList("a"), Arrays.asList("Int8"));
        assertEquals(Types.TINYINT, meta.getColumnType(1));
    }

    @Test
    void typeMapping_Int16() throws Exception {
        var meta = makeMetaData(Arrays.asList("a"), Arrays.asList("Int16"));
        assertEquals(Types.SMALLINT, meta.getColumnType(1));
    }

    @Test
    void typeMapping_UInt8() throws Exception {
        var meta = makeMetaData(Arrays.asList("a"), Arrays.asList("UInt8"));
        assertEquals(Types.SMALLINT, meta.getColumnType(1));
    }

    @Test
    void typeMapping_Int32() throws Exception {
        var meta = makeMetaData(Arrays.asList("a"), Arrays.asList("Int32"));
        assertEquals(Types.INTEGER, meta.getColumnType(1));
    }

    @Test
    void typeMapping_UInt16() throws Exception {
        var meta = makeMetaData(Arrays.asList("a"), Arrays.asList("UInt16"));
        assertEquals(Types.INTEGER, meta.getColumnType(1));
    }

    @Test
    void typeMapping_Int64() throws Exception {
        var meta = makeMetaData(Arrays.asList("a"), Arrays.asList("Int64"));
        assertEquals(Types.BIGINT, meta.getColumnType(1));
    }

    @Test
    void typeMapping_UInt32() throws Exception {
        var meta = makeMetaData(Arrays.asList("a"), Arrays.asList("UInt32"));
        assertEquals(Types.BIGINT, meta.getColumnType(1));
    }

    @Test
    void typeMapping_UInt64() throws Exception {
        var meta = makeMetaData(Arrays.asList("a"), Arrays.asList("UInt64"));
        assertEquals(Types.NUMERIC, meta.getColumnType(1));
    }

    @Test
    void typeMapping_Float32() throws Exception {
        var meta = makeMetaData(Arrays.asList("a"), Arrays.asList("Float32"));
        assertEquals(Types.FLOAT, meta.getColumnType(1));
    }

    @Test
    void typeMapping_Float64() throws Exception {
        var meta = makeMetaData(Arrays.asList("a"), Arrays.asList("Float64"));
        assertEquals(Types.DOUBLE, meta.getColumnType(1));
    }

    @Test
    void typeMapping_String() throws Exception {
        var meta = makeMetaData(Arrays.asList("a"), Arrays.asList("String"));
        assertEquals(Types.VARCHAR, meta.getColumnType(1));
    }

    @Test
    void typeMapping_FixedString() throws Exception {
        var meta = makeMetaData(Arrays.asList("a"), Arrays.asList("FixedString(36)"));
        assertEquals(Types.VARCHAR, meta.getColumnType(1));
    }

    @Test
    void typeMapping_Date() throws Exception {
        var meta = makeMetaData(Arrays.asList("a"), Arrays.asList("Date"));
        assertEquals(Types.DATE, meta.getColumnType(1));
    }

    @Test
    void typeMapping_DateTime() throws Exception {
        var meta = makeMetaData(Arrays.asList("a"), Arrays.asList("DateTime"));
        assertEquals(Types.TIMESTAMP, meta.getColumnType(1));
    }

    @Test
    void typeMapping_DateTime64() throws Exception {
        var meta = makeMetaData(Arrays.asList("a"), Arrays.asList("DateTime64(3)"));
        assertEquals(Types.TIMESTAMP, meta.getColumnType(1));
    }

    @Test
    void typeMapping_UUID() throws Exception {
        var meta = makeMetaData(Arrays.asList("a"), Arrays.asList("UUID"));
        assertEquals(Types.VARCHAR, meta.getColumnType(1));
    }

    @Test
    void typeMapping_NullableInt32() throws Exception {
        var meta = makeMetaData(Arrays.asList("a"), Arrays.asList("Nullable(Int32)"));
        assertEquals(Types.INTEGER, meta.getColumnType(1));
    }

    @Test
    void typeMapping_NullableString() throws Exception {
        var meta = makeMetaData(Arrays.asList("a"), Arrays.asList("Nullable(String)"));
        assertEquals(Types.VARCHAR, meta.getColumnType(1));
    }

    @Test
    void typeMapping_Unknown() throws Exception {
        var meta = makeMetaData(Arrays.asList("a"), Arrays.asList("SomeUnknownType"));
        assertEquals(Types.VARCHAR, meta.getColumnType(1));
    }

    @Test
    void isNullable_nonNullable() throws Exception {
        var meta = makeMetaData(Arrays.asList("a"), Arrays.asList("Int32"));
        assertEquals(ResultSetMetaData.columnNoNulls, meta.isNullable(1));
    }

    @Test
    void isNullable_nullable() throws Exception {
        var meta = makeMetaData(Arrays.asList("a"), Arrays.asList("Nullable(Int32)"));
        assertEquals(ResultSetMetaData.columnNullable, meta.isNullable(1));
    }

    @Test
    void getPrecision() throws Exception {
        var meta = makeMetaData(Arrays.asList("a", "b"), Arrays.asList("Int32", "Float64"));
        assertEquals(10, meta.getPrecision(1));
        assertEquals(15, meta.getPrecision(2));
    }

    @Test
    void getScale() throws Exception {
        var meta = makeMetaData(Arrays.asList("a", "b"), Arrays.asList("Int32", "Float64"));
        assertEquals(0, meta.getScale(1));
        assertEquals(15, meta.getScale(2));
    }
}
