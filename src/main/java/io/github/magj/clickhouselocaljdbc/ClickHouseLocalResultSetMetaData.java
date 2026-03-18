package io.github.magj.clickhouselocaljdbc;

import java.sql.*;
import java.util.List;

public class ClickHouseLocalResultSetMetaData implements ResultSetMetaData {

    private final List<String> columnNames;
    private final List<String> columnTypes;

    public ClickHouseLocalResultSetMetaData(List<String> columnNames, List<String> columnTypes) {
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columnNames.size();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return columnNames.get(column - 1);
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return columnTypes.get(column - 1);
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        String type = columnTypes.get(column - 1);
        return mapClickHouseTypeToSqlType(type);
    }

    private int mapClickHouseTypeToSqlType(String type) {
        String baseType = unwrapNullable(type);
        if (baseType.equals("Int8")) return Types.TINYINT;
        if (baseType.equals("Int16") || baseType.equals("UInt8")) return Types.SMALLINT;
        if (baseType.equals("Int32") || baseType.equals("UInt16")) return Types.INTEGER;
        if (baseType.equals("Int64") || baseType.equals("UInt32")) return Types.BIGINT;
        if (baseType.equals("UInt64")) return Types.NUMERIC;
        if (baseType.equals("Float32")) return Types.FLOAT;
        if (baseType.equals("Float64")) return Types.DOUBLE;
        if (baseType.equals("String") || baseType.startsWith("FixedString(")) return Types.VARCHAR;
        if (baseType.equals("Date")) return Types.DATE;
        if (baseType.equals("DateTime") || baseType.startsWith("DateTime64(")) return Types.TIMESTAMP;
        if (baseType.equals("UUID")) return Types.VARCHAR;
        return Types.VARCHAR;
    }

    private String unwrapNullable(String type) {
        if (type.startsWith("Nullable(") && type.endsWith(")")) {
            return type.substring(9, type.length() - 1);
        }
        return type;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        String type = columnTypes.get(column - 1);
        if (type.startsWith("Nullable(")) {
            return columnNullable;
        }
        return columnNoNulls;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        String type = unwrapNullable(columnTypes.get(column - 1));
        return type.startsWith("Int") || type.startsWith("Float");
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 255;
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        String baseType = unwrapNullable(columnTypes.get(column - 1));
        if (baseType.equals("Int8")) return 3;
        if (baseType.equals("Int16") || baseType.equals("UInt8")) return 5;
        if (baseType.equals("Int32") || baseType.equals("UInt16")) return 10;
        if (baseType.equals("Int64") || baseType.equals("UInt32")) return 19;
        if (baseType.equals("UInt64")) return 20;
        if (baseType.equals("Float32")) return 7;
        if (baseType.equals("Float64")) return 15;
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        String baseType = unwrapNullable(columnTypes.get(column - 1));
        if (baseType.equals("Float32")) return 7;
        if (baseType.equals("Float64")) return 15;
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        String baseType = unwrapNullable(columnTypes.get(column - 1));
        if (baseType.equals("Int8")) return Byte.class.getName();
        if (baseType.equals("Int16") || baseType.equals("UInt8")) return Short.class.getName();
        if (baseType.equals("Int32") || baseType.equals("UInt16")) return Integer.class.getName();
        if (baseType.equals("Int64") || baseType.equals("UInt32")) return Long.class.getName();
        if (baseType.equals("UInt64")) return java.math.BigDecimal.class.getName();
        if (baseType.equals("Float32")) return Float.class.getName();
        if (baseType.equals("Float64")) return Double.class.getName();
        if (baseType.equals("Date")) return java.sql.Date.class.getName();
        if (baseType.equals("DateTime") || baseType.startsWith("DateTime64(")) return java.sql.Timestamp.class.getName();
        return String.class.getName();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }
}
