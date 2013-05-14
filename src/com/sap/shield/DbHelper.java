package com.sap.shield;

import com.sap.shield.data.DataConverter;
import com.sap.shield.exceptions.ShieldException;
import com.sap.shield.messages.CreateTableRequest;
import com.sap.shield.messages.UpdateTableRequest;
import com.sap.shield.rdb.HanaConnectionPool;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/29/12
 * Time: 4:45 PM
 * To change this template use File | Settings | File Templates.
 */
public final class DbHelper {
    public static final String TABLE_SQL = " SELECT ID, NAME, PARSER_CLASS, APPEND_TIMESTAMP, CREATED FROM SHIELD.SHIELD_TABLES ";
    public static final String COLUMN_SQL = " SELECT ID, NAME, DATA_TYPE FROM SHIELD.SHIELD_COLUMNS ";
    public static final String TABLE_COLUMN_SQL = " SELECT TABLE_ID, COLUMN_ID, ORDER_NUM, ROW_KEY_ORDER FROM SHIELD.SHIELD_TABLE_COLUMNS ";
    public static final String INSERT_HOST = "INSERT INTO SHIELD.SHIELD_SERVERS (FULL_HOST_NAME, UPDATE_SEQUENCE, UPDATE_URL) SELECT ?, (SELECT IFNULL(MAX(UPDATE_SEQUENCE), 0) FROM SHIELD.SHIELD_SERVERS), ? FROM SHIELD.SHIELD_SERVERS LIMIT 1";
    public static final String INSERT_FIRST_HOST = "INSERT INTO SHIELD.SHIELD_SERVERS (FULL_HOST_NAME, UPDATE_SEQUENCE, UPDATE_URL) VALUES (?, ?, ?) ";
    public static final String GET_HOST_COUNT = "SELECT COUNT(0) FROM SHIELD.SHIELD_SERVERS WHERE LOWER(FULL_HOST_NAME) = ? ";
    public static final String GET_ALL_HOST_COUNT = "SELECT COUNT(0) FROM SHIELD.SHIELD_SERVERS ";
    public static final String GET_ALL_HOST_UPDATE_URLS = "SELECT UPDATE_URL FROM SHIELD.SHIELD_SERVERS ";
    public static final String SYNC_HOST_SEQUENCE = "UPDATE SHIELD.SHIELD_SERVERS SET UPDATE_SEQUENCE = (SELECT MAX(UPDATE_SEQUENCE) FROM SHIELD.SHIELD_SERVERS) WHERE LOWER(FULL_HOST_NAME) = ? ";
    public static final String UPDATE_HOST_SEQUENCE = "UPDATE SHIELD.SHIELD_SERVERS SET UPDATE_SEQUENCE = (UPDATE_SEQUENCE + 1) WHERE LOWER(FULL_HOST_NAME) = ? ";

    private static final String INSERT_TABLE_SQL = " INSERT INTO SHIELD.SHIELD_TABLES (ID, NAME, PARSER_CLASS, APPEND_TIMESTAMP, CREATED) VALUES (?, ?, ?, ?, ?)";
    private static final String INSERT_COLUMN_SQL = " INSERT INTO SHIELD.SHIELD_COLUMNS VALUES (?, ?, ?)";
    private static final String INSERT_TABLE_COLUMN_SQL = " INSERT INTO SHIELD.SHIELD_TABLE_COLUMNS VALUES (?, ?, ?, ?)";
    private static final String DELETE_TABLE_COLUMN_SQL = " DELETE FROM SHIELD.SHIELD_TABLE_COLUMNS WHERE TABLE_ID = ? ";
    private static final String DELETE_COLUMN_SQL = " DELETE FROM SHIELD.SHIELD_COLUMNS WHERE ID = ? ";
    private static final String DELETE_COLUMN_BY_TABLE_SQL = " DELETE FROM SHIELD.SHIELD_COLUMNS WHERE ID IN (SELECT ID FROM SHIELD.SHIELD_TABLE_COLUMNS WHERE TABLE_ID = ?)";
    private static final String DELETE_TABLE_SQL = " DELETE FROM SHIELD.SHIELD_TABLES WHERE ID = ? ";
    private static final String CHECK_TABLE_EXISTENCE_SQL = "SELECT COUNT(0) FROM TABLES WHERE TABLE_NAME = ? and SCHEMA_NAME = 'SHIELD'";

    // Singleton
    private static final DbHelper INSTANCE = new DbHelper();

    private DbHelper() {
    }

    public static DbHelper getInstance() {
        return INSTANCE;
    }

    public void createLoadingTable(String tableName,
                                   List<String> columnNames,
                                   List<DataConverter> dataConverters) throws SQLException {
        Connection conn = HanaConnectionPool.getInstance().getConnection();
        dropLoadingTable(conn, tableName);
        createLoadingTable(conn, tableName, columnNames, dataConverters);
        HanaConnectionPool.getInstance().returnConnection(conn);
    }

    private void dropLoadingTable(Connection conn, String tableName) throws SQLException {
        tableName = tableName.toUpperCase();
        if (tableName.startsWith("SHIELD.")) {
            tableName = tableName.replace("SHIELD.", "");
        }
        System.out.println("Check table sql: " + CHECK_TABLE_EXISTENCE_SQL);
        PreparedStatement checkStmt = conn.prepareStatement(CHECK_TABLE_EXISTENCE_SQL);
        checkStmt.setString(1, tableName);
        ResultSet rs = checkStmt.executeQuery();
        rs.next();
        int tableCount = rs.getInt(1);
        rs.close();
        checkStmt.close();

        if (tableCount > 0) {
            if (!tableName.startsWith("SHIELD.")) {
                tableName = "SHIELD." + tableName;
            }
            String sql = " DROP TABLE " + tableName;
            System.out.println("Drop table sql: " + sql);
            PreparedStatement truncateTableStmt = conn.prepareStatement(sql);
            truncateTableStmt.execute();
            truncateTableStmt.close();
        }
    }

    private void createLoadingTable(Connection conn, String tableName,
                                    List<String> columnNames,
                                    List<DataConverter> dataConverters) throws SQLException {
        tableName = tableName.toUpperCase();
        if (!tableName.startsWith("SHIELD.")) {
            tableName = "SHIELD." + tableName;
        }

        StringBuilder sql = new StringBuilder(" CREATE COLUMN TABLE ").append(tableName).append("\n");
        for (int i = 0; i < columnNames.size(); i++) {
            if (i == 0) {
                sql.append(" ( ");
            }
            sql.append(columnNames.get(i)).append(" ").append(dataConverters.get(i).getDbTypeName());
            if (i < columnNames.size() - 1) {
                sql.append(",");
            }
            if (i == columnNames.size() - 1) {
                sql.append(" ) ");
            }
        }
        System.out.println("Create Table SQL: \n" + sql.toString());
        PreparedStatement statement = conn.prepareStatement(sql.toString());
        statement.executeUpdate();
        statement.close();
    }

    public int runUpdate(Connection conn, String query, List<Object> values) throws SQLException {
        PreparedStatement statement = conn.prepareStatement(query);
        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            if (value instanceof String) {
                statement.setString(i + 1, (String) value);
            } else if (value instanceof Timestamp) {
                statement.setTimestamp(i + 1, (Timestamp) value);
            } else if (value instanceof Integer) {
                statement.setInt(i + 1, (Integer) value);
            }
        }
        int rowUpdated = statement.executeUpdate();
        statement.close();
        return rowUpdated;
    }

    public List<Object[]> getResultSet(Connection conn, String query, List<Object> values) throws SQLException {
        List<Object[]> resultList = new ArrayList<Object[]>();
        PreparedStatement statement = conn.prepareStatement(query);
        if (values != null) {
            for (int i = 0; i < values.size(); i++) {
                Object value = values.get(i);
                if (value instanceof String) {
                    statement.setString(i + 1, (String) value);
                } else if (value instanceof Timestamp) {
                    statement.setTimestamp(i + 1, (Timestamp) value);
                } else if (value instanceof Integer) {
                    statement.setInt(i + 1, (Integer) value);
                }
            }
        }
        ResultSet rs = statement.executeQuery();
        int columnCount = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            Object[] objs = new Object[columnCount];
            for (int i = 0; i < columnCount; i++) {
                int type = rs.getMetaData().getColumnType(i + 1);
                if (type == Types.INTEGER) {
                    objs[i] = rs.getInt(i + 1);
                } else if (type == Types.TIMESTAMP) {
                    objs[i] = rs.getTimestamp(i + 1);
                } else if (type == Types.VARCHAR) {
                    objs[i] = rs.getString(i + 1);
                } else if (type == Types.BIGINT) {
                    objs[i] = rs.getLong(i + 1);
                }
            }
            resultList.add(objs);
        }
        rs.close();
        statement.close();
        return resultList;
    }

    public void createShieldTable(CreateTableRequest request) throws SQLException, ShieldException {
        Connection conn = HanaConnectionPool.getInstance().getConnection();
        System.out.println(request.getPropertyBoolean(Constants.APPEND_TIMESTAMP, true));
        // Upsert the event table in db
        insertShieldTable(conn, request.getPropertyInt(Constants.TABLE_ID), request.getPropertyString(Constants.TABLE_NAME), request.getPropertyString("parserClassName"), request.getPropertyBoolean(Constants.APPEND_TIMESTAMP, true), request.getPropertyTimestamp("timestamp"));

        // Upsert the event columns in db
        insertShieldColumns(conn, request.getPropertyIntArray("columnIds"), request.getPropertyStringList("columnNames"), request.getPropertyStringList("columnTypes"));

        // Upsert the event table-columns in db
        insertShieldTableColumns(conn, request.getPropertyInt(Constants.TABLE_ID), request.getPropertyIntArray("columnIds"),
                request.getPropertyIntArray("orderNums"), request.getPropertyIntegerList("columnRowKeyOrder"));
        HanaConnectionPool.getInstance().returnConnection(conn);
    }

    public void updateShieldTable(UpdateTableRequest request) throws SQLException, ShieldException {
        Connection conn = HanaConnectionPool.getInstance().getConnection();

        deleteShieldTableColumns(conn, request.getPropertyInt(Constants.TABLE_ID));
        deleteShieldColumns(conn, request.getPropertyIntArray("oldColumnIds"));

        // Upsert the event columns in db
        insertShieldColumns(conn, request.getPropertyIntArray("columnIds"), request.getPropertyStringList("columnNames"), request.getPropertyStringList("columnTypes"));

        // Upsert the event table-columns in db
        insertShieldTableColumns(conn, request.getPropertyInt(Constants.TABLE_ID), request.getPropertyIntArray("columnIds"), request.getPropertyIntArray("orderNums"), request.getPropertyIntegerList("columnRowKeyOrder"));
        HanaConnectionPool.getInstance().returnConnection(conn);
    }


    private void deleteShieldColumns(Connection conn, int[] cids) throws SQLException {
        PreparedStatement preparedStatement = conn.prepareStatement(DELETE_COLUMN_SQL);
        for (int cid : cids) {
            preparedStatement.setInt(1, cid);
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
        preparedStatement.close();
    }

    private void deleteShieldTableColumns(Connection conn, int tid) throws SQLException {
        PreparedStatement preparedStatement = conn.prepareStatement(DELETE_TABLE_COLUMN_SQL);
        preparedStatement.setInt(1, tid);
        preparedStatement.execute();
        preparedStatement.close();
    }

    public void deleteTable(int tableId) throws SQLException {
        Connection conn = HanaConnectionPool.getInstance().getConnection();
        runUpdate(conn, DELETE_COLUMN_BY_TABLE_SQL, Arrays.asList((Object) String.valueOf(tableId)));
        runUpdate(conn, DELETE_TABLE_COLUMN_SQL, Arrays.asList((Object) String.valueOf(tableId)));
        runUpdate(conn, DELETE_TABLE_SQL, Arrays.asList((Object) String.valueOf(tableId)));
        HanaConnectionPool.getInstance().returnConnection(conn);
        ShieldManager.getInstance().addUpdateSequence();
    }

    private boolean insertShieldTable(Connection conn, int id, String name, String parser, boolean appendTs, Timestamp ts) throws SQLException {
        PreparedStatement preparedStatement = conn.prepareStatement(INSERT_TABLE_SQL);
        preparedStatement.setInt(1, id);
        preparedStatement.setString(2, name);
        preparedStatement.setString(3, parser);
        if (appendTs) {
            preparedStatement.setString(4, "true");
        } else {
            preparedStatement.setString(4, "false");
        }
        preparedStatement.setTimestamp(5, ts);
        boolean insertOk = preparedStatement.execute();
        preparedStatement.close();
        return insertOk;
    }

    private void insertShieldColumns(Connection conn, int[] cids, List<String> columnNames, List<String> columnDataTypes) throws SQLException {
        PreparedStatement preparedStatement = conn.prepareStatement(INSERT_COLUMN_SQL);
        for (int i = 0; i < columnNames.size(); i++) {
            preparedStatement.setInt(1, cids[i]);
            preparedStatement.setString(2, columnNames.get(i));
            preparedStatement.setString(3, columnDataTypes.get(i));
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
        preparedStatement.close();
    }

    private void insertShieldTableColumns(Connection conn, int tableId, int[] columnIds, int[] orderNums, List<Integer> rowKeyOrder) throws SQLException {
        PreparedStatement preparedStatement = conn.prepareStatement(INSERT_TABLE_COLUMN_SQL);
        for (int i = 0; i < columnIds.length; i++) {
            preparedStatement.setInt(1, tableId);
            preparedStatement.setInt(2, columnIds[i]);
            preparedStatement.setInt(3, orderNums[i]);
            preparedStatement.setInt(4, rowKeyOrder.get(i));
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
        preparedStatement.close();
    }

    public int getMaxId(Connection conn, String query) throws SQLException {
        int maxTableId = 0;
        PreparedStatement preparedStatement = conn.prepareStatement(query);
        ResultSet resultSet = preparedStatement.executeQuery();

        if (resultSet.next()) {
            maxTableId = resultSet.getInt(1);
        }
        resultSet.close();
        preparedStatement.close();
        return maxTableId;
    }
}
