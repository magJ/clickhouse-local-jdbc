package io.github.magj.clickhouselocaljdbc;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ClickHouseLocalStatement implements Statement {

    protected final ClickHouseLocalConnection connection;
    protected final String clickhouseLocalPath;
    protected final String workingDirectory;

    private boolean closed = false;
    private ResultSet lastResultSet = null;
    private int updateCount = -1;

    public ClickHouseLocalStatement(ClickHouseLocalConnection connection) {
        this.connection = connection;
        this.clickhouseLocalPath = connection.getClickhouseLocalPath();
        this.workingDirectory = connection.getWorkingDirectory();
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkClosed();
        List<String> cmd = new ArrayList<>();
        cmd.add(clickhouseLocalPath);
        cmd.add("--query");
        cmd.add(sql);
        cmd.add("--output-format");
        cmd.add("TabSeparatedWithNamesAndTypes");
        String output = runProcess(cmd);
        lastResultSet = parseTabSeparatedOutput(output);
        updateCount = -1;
        return lastResultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        executeQuery(sql);
        lastResultSet = null;
        updateCount = 0;
        return 0;
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        executeQuery(sql);
        ClickHouseLocalResultSet rs = (ClickHouseLocalResultSet) lastResultSet;
        if (rs != null && rs.getColumnCount() > 0) {
            updateCount = -1;
            return true;
        } else {
            lastResultSet = null;
            updateCount = 0;
            return false;
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return lastResultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return updateCount;
    }

    protected String runProcess(List<String> command) throws SQLException {
        ProcessBuilder pb = new ProcessBuilder(command);
        if (workingDirectory != null) {
            pb.directory(new File(workingDirectory));
        }
        try {
            Process process = pb.start();
            String stdout = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            String stderr = new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new SQLException("clickhouse-local failed: " + stderr);
            }
            return stdout;
        } catch (IOException | InterruptedException e) {
            throw new SQLException("Failed to run clickhouse-local: " + e.getMessage(), e);
        }
    }

    public ClickHouseLocalResultSet parseTabSeparatedOutput(String output) {
        String[] lines = output.split("\n", -1);
        if (lines.length < 2) {
            return emptyResultSet();
        }
        String[] columnNames = lines[0].split("\t", -1);
        String[] columnTypes = lines[1].split("\t", -1);
        List<String[]> rows = new ArrayList<>();
        for (int i = 2; i < lines.length; i++) {
            if (!lines[i].isEmpty()) {
                String[] rawFields = lines[i].split("\t", -1);
                String[] fields = new String[rawFields.length];
                for (int j = 0; j < rawFields.length; j++) {
                    fields[j] = unescapeTabSeparatedValue(rawFields[j]);
                }
                rows.add(fields);
            }
        }
        return new ClickHouseLocalResultSet(
            Arrays.asList(columnNames), Arrays.asList(columnTypes), rows);
    }

    private static String unescapeTabSeparatedValue(String s) {
        if ("\\N".equals(s)) {
            return null;
        }
        if (s.indexOf('\\') == -1) {
            return s;
        }
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char next = s.charAt(++i);
                switch (next) {
                    case 't': sb.append('\t'); break;
                    case 'n': sb.append('\n'); break;
                    case 'r': sb.append('\r'); break;
                    case '\\': sb.append('\\'); break;
                    case '0': sb.append('\0'); break;
                    case 'b': sb.append('\b'); break;
                    case 'f': sb.append('\f'); break;
                    case 'a': sb.append('\u0007'); break;
                    default: sb.append('\\'); sb.append(next); break;
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private ClickHouseLocalResultSet emptyResultSet() {
        return new ClickHouseLocalResultSet(
            new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    }

    @Override
    public void close() throws SQLException {
        closed = true;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Statement is closed");
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        // no-op
    }

    @Override
    public int getMaxRows() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        // no-op
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        // no-op
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        // no-op
    }

    @Override
    public void cancel() throws SQLException {
        // no-op
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        // no-op
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("setCursorName not supported");
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        // no-op
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        // no-op
    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("addBatch not supported");
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("clearBatch not supported");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("executeBatch not supported");
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException("getGeneratedKeys not supported");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return execute(sql);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        // no-op
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        // no-op
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
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
