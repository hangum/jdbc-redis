package br.com.svvs.jdbc.redis;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class RedisStatement extends RedisAbstractStatement implements Statement {

    // used to batch operations..
    private List<String> batchOps = new ArrayList<String>();

    public RedisStatement(RedisConnection conn) {
        super(conn);
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        this.batchOps.add(sql);
    }

    @Override
    public void cancel() throws SQLException {
    }

    @Override
    public void clearBatch() throws SQLException {
        this.batchOps.clear();
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public void close() throws SQLException {
        this.conn = null;
        this.isClosed = true;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys)
            throws SQLException {
        return this.execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return this.execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames)
            throws SQLException {
        return this.execute(sql);
    }

    @Override
    public int[] executeBatch() throws SQLException {

        int[] results = new int[this.batchOps.size()];

        int ops    = 0;
        for(String sql : this.batchOps) {
            try {
                this.execute(sql);
                results[ops] = SUCCESS_NO_INFO;
            } catch(SQLException e) {
                throw new BatchUpdateException(e);
            }
            ops++;
        }

        return results;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        this.execute(sql);
        return this.getResultSet();
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {

        try {
            RedisCommandWrapper wrapper = this.extractCommand(sql);

            String redisMsg = wrapper.cmd.createMsg(wrapper.value);

            String[] result = wrapper.cmd.parseMsg(this.conn.msgToServer(redisMsg));

            if(result != null && result.length == 1) {
                return Integer.parseInt(result[0]);
            } else {
                return 0;
            }

        } catch (RedisParseException e) {
            throw new SQLException(e);
        } catch (RedisResultException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys)
            throws SQLException {
        return this.executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes)
            throws SQLException {
        return this.executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames)
            throws SQLException {
        return this.executeUpdate(sql);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.conn;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRows() throws SQLException {
        return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return this.resultSet;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return -1;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.isClosed;
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void setCursorName(String name) throws SQLException {
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
    }

    @Override
    public void setPoolable(boolean arg0) throws SQLException {
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

}
