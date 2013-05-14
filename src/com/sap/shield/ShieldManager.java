package com.sap.shield;

import com.sap.shield.cache.ShieldColumns;
import com.sap.shield.cache.ShieldTableColumns;
import com.sap.shield.cache.ShieldTables;
import com.sap.shield.exceptions.ShieldException;
import com.sap.shield.ext.parsers.Parser;
import com.sap.shield.messages.CreateTableRequest;
import com.sap.shield.messages.UpdateTableRequest;
import com.sap.shield.rdb.HanaConnectionPool;
import com.sap.shield.rest.client.ShieldHttpClient;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/31/12
 * Time: 10:46 AM
 * To change this template use File | Settings | File Templates.
 */
public final class ShieldManager {

    private static final Logger LOG = Logger.getLogger(ShieldManager.class.getName());

    private static final ShieldManager INSTANCE = new ShieldManager();

    private ShieldManager() {
        updateAll();
    }

    public AtomicBoolean refreshing = new AtomicBoolean(false);

    public boolean isRefreshing() {
        return refreshing.get();
    }

    public synchronized String getMovedUrl(String path) {
        if (Constants.MASTER_SCHEME == null || Constants.MASTER_URL == null) {
            return null;
        }

        if (ShieldManager.getInstance().isRefreshing()) {
            String target = Constants.MASTER_SCHEME + "://" + Constants.MASTER_URL + ":" + Constants.MASTER_PORT;
            if (path != null) {
                target += path;
            } else {
                target += "/";
            }
            return target;
        }
        return null;
    }

    public void setRefreshing(boolean refreshing) {
        this.refreshing.set(refreshing);
    }

    public void updateAll() {
        try {
            if (StringUtils.isEmpty(Constants.HOSTNAME)) {
                InetAddress inetAddress = InetAddress.getLocalHost();
                Constants.HOSTNAME = inetAddress.getHostName();
            }
            Connection conn = HanaConnectionPool.getInstance().getConnection();
            populateCache(conn, Constants.HOSTNAME, Application.getUpdateUrl(Constants.HOSTNAME));
            HanaConnectionPool.getInstance().returnConnection(conn);
        } catch (SQLException e) {
            LOG.log(Level.SEVERE, "ShieldManager()", e);
            System.exit(1);
        } catch (UnknownHostException he) {
            LOG.log(Level.SEVERE, "ShieldManager()", he);
            System.exit(1);
        }
    }

    public synchronized void addUpdateSequence(Connection conn) throws SQLException {
        DbHelper.getInstance().runUpdate(conn, DbHelper.UPDATE_HOST_SEQUENCE, Arrays.asList((Object) Constants.HOSTNAME.toLowerCase()));
        notifyHosts(conn);
    }

    public synchronized void addUpdateSequence() throws SQLException {
        Connection conn = HanaConnectionPool.getInstance().getConnection();
        addUpdateSequence(conn);
        HanaConnectionPool.getInstance().returnConnection(conn);
    }

    public static ShieldManager getInstance() {
        return INSTANCE;
    }

    public void populateCache(Connection conn, String hostname, String updateUrl) throws SQLException {
        populateShieldTablesCache(conn);
        populateShieldColumnsCache(conn);
        populateShieldTableColumnsCache(conn);
        upsertHost(conn, hostname, updateUrl);
    }

    private void upsertHost(Connection conn, String hostname, String updateUrl) throws SQLException {
        List<Object[]> allHostRsList = DbHelper.getInstance().getResultSet(conn, DbHelper.GET_ALL_HOST_COUNT, null);
        List<Object[]> hostRsList = DbHelper.getInstance().getResultSet(conn, DbHelper.GET_HOST_COUNT, Arrays.asList((Object) hostname.toLowerCase()));
        long hostCount = hostRsList.get(0)[0] != null ? (Long) hostRsList.get(0)[0] : 0;
        long allHostCount = allHostRsList.get(0)[0] != null ? (Long) allHostRsList.get(0)[0] : 0;
        if (hostCount == 0) {
            if (allHostCount > 0) {
                DbHelper.getInstance().runUpdate(conn, DbHelper.INSERT_HOST, Arrays.asList((Object) hostname.toLowerCase(), (Object) updateUrl));
            } else {
                DbHelper.getInstance().runUpdate(conn, DbHelper.INSERT_FIRST_HOST, Arrays.asList((Object) hostname.toLowerCase(), (Object) Integer.parseInt("4"), (Object) updateUrl));
            }
        } else {
            DbHelper.getInstance().runUpdate(conn, DbHelper.SYNC_HOST_SEQUENCE, Arrays.asList((Object) hostname.toLowerCase()));
        }
    }

    public void notifyHosts(Connection conn) throws SQLException {
        List<Object[]> updateUrls = DbHelper.getInstance().getResultSet(conn, DbHelper.GET_ALL_HOST_UPDATE_URLS, null);
        for (Object[] objs : updateUrls) {
            ShieldHttpClient shieldHttpClient = new ShieldHttpClient();
            String updateUrl = (String) objs[0];
            try {
                shieldHttpClient.sendGet(updateUrl);
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
    }

    private void populateShieldTablesCache(Connection conn) throws SQLException {
        LOG.log(Level.INFO, "About to populating Shield Table Cache from DB: " + DbHelper.TABLE_SQL);
        PreparedStatement preparedStatement1 = conn.prepareStatement(DbHelper.TABLE_SQL);
        ResultSet rs = preparedStatement1.executeQuery();
        while (rs.next()) {
            ShieldTables.Row row =
                    new ShieldTables.Row(rs.getInt(1), rs.getString(2), rs.getString(3),
                            Boolean.parseBoolean(rs.getString(4)), rs.getTimestamp(5));
            ShieldTables.upsertRow(row);
        }
        preparedStatement1.close();
    }

    private void populateShieldColumnsCache(Connection conn) throws SQLException {
        LOG.log(Level.INFO, "About to populating Shield Column Cache from DB: " + DbHelper.COLUMN_SQL);
        PreparedStatement preparedStatement1 = conn.prepareStatement(DbHelper.COLUMN_SQL);
        ResultSet rs = preparedStatement1.executeQuery();
        while (rs.next()) {
            ShieldColumns.Row row = new ShieldColumns.Row(rs.getInt(1), rs.getString(2), rs.getString(3));
            ShieldColumns.upsertRow(row);
        }
        preparedStatement1.close();
    }

    private void populateShieldTableColumnsCache(Connection conn) throws SQLException {
        LOG.log(Level.INFO, "About to populating Shield Table Column Cache from DB: " + DbHelper.TABLE_COLUMN_SQL);
        PreparedStatement preparedStatement1 = conn.prepareStatement(DbHelper.TABLE_COLUMN_SQL);
        ResultSet rs = preparedStatement1.executeQuery();
        while (rs.next()) {
            ShieldTableColumns.Row row = new ShieldTableColumns.Row(rs.getInt(1), rs.getInt(2), rs.getInt(3), rs.getInt(4));
            ShieldTableColumns.upsertRow(row);
        }
        preparedStatement1.close();
    }

    public void createShieldTable(CreateTableRequest request) throws SQLException, ShieldException {
        request.addProperty("maxTableId", ShieldTables.getMaxId());
        request.addProperty("maxColumnId", ShieldColumns.getMaxId());
        request.addProperty("timestamp", new Timestamp(System.currentTimeMillis()));
        CacheHelper.getInstance().createTable(request);
        DbHelper.getInstance().createShieldTable(request);
    }

    public synchronized void updateShieldTable(UpdateTableRequest request) throws SQLException, ShieldException {
        CacheHelper.getInstance().updateShieldTable(request);
        DbHelper.getInstance().updateShieldTable(request);
    }

    public synchronized void deleteShieldTable(int tableId) throws SQLException, IOException, ShieldException {
        String tableName = ShieldTables.getTableName(tableId);
        if (HbaseHelper.getInstance().tableExists(tableName)) {
            HbaseHelper.getInstance().disableTable(tableName);
            HbaseHelper.getInstance().dropTable(tableName);
        }
        CacheHelper.getInstance().deleteTable(tableId);
        DbHelper.getInstance().deleteTable(tableId);
    }

    public Parser getParser(String name) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        if (name == null) {
            return null;
        }
        Class theClass = Class.forName(name);
        return (Parser) theClass.newInstance();
    }
}
