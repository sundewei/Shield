package com.sap.shield.rdb;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/29/12
 * Time: 10:57 AM
 * To change this template use File | Settings | File Templates.
 */
public final class HanaConnectionPool implements ConnectionPool {
    private static final String CONNECTION_STRING = "jdbc:sap://LSPAL134.pal.sap.corp:31015";
    private static final String DB_USERNAME = "SYSTEM";
    private static final String DB_PASSWORD = "Hana1234";
    //private static final String CONNECTION_STRING = "jdbc:sap://10.165.27.75:31015?reconnect=true";
    //private static final String DB_USERNAME = "SYSTEM";
    //private static final String DB_PASSWORD = "Admin123";


    private static final String DB_DRIVER = "com.sap.db.jdbc.Driver";
    private static final Logger LOG = Logger.getLogger(HanaConnectionPool.class.getName());
    private static GenericObjectPool OBJECT_POOL;
    private static final int MAX_CONNECTION_IN_POOL = 10;
    private static DataSource DATA_SOURCE;
    private static HanaConnectionPool INSTANCE;

    static {
        DATA_SOURCE = init();
        INSTANCE = new HanaConnectionPool();
    }

    private HanaConnectionPool() {
    }

    public static HanaConnectionPool getInstance() {
        return INSTANCE;
    }

    private static DataSource init() {
        //
        // Creates an instance of GenericObjectPool that holds our
        // pool of connections object.
        //
        OBJECT_POOL = new GenericObjectPool();
        OBJECT_POOL.setMaxActive(MAX_CONNECTION_IN_POOL);

        //
        // Creates a connection factory object which will be use by
        // the pool to create the connection object. We passes the
        // JDBC url info, username and password.
        //
        try {
            DriverManager.registerDriver((Driver) Class.forName(DB_DRIVER).newInstance());
        } catch (Exception ce) {
            LOG.log(Level.SEVERE, ce.getMessage(), ce);
        }
        ConnectionFactory cf = new DriverManagerConnectionFactory(CONNECTION_STRING, DB_USERNAME, DB_PASSWORD);

        //
        // Creates a PoolableConnectionFactory that will wraps the
        // connection object created by the ConnectionFactory to add
        // object pooling functionality.
        //
        PoolableConnectionFactory pcf = new PoolableConnectionFactory(cf, OBJECT_POOL, null, null, false, true);
        return new PoolingDataSource(OBJECT_POOL);
    }


    public Connection getConnection() throws SQLException {
        return DATA_SOURCE.getConnection();
    }

    public void returnConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.clearWarnings();
            } catch (SQLException se) {
                LOG.warning(se.getMessage());
            }

            try {
                conn.close();
            } catch (SQLException se) {
                LOG.warning(se.getMessage());
            }
        }
    }

    public static void main(String[] arg) throws Exception {
        HanaConnectionPool hanaConnectionPool = HanaConnectionPool.getInstance();

        Connection conn1 = hanaConnectionPool.getConnection();
        Connection conn2 = hanaConnectionPool.getConnection();
        Connection conn3 = hanaConnectionPool.getConnection();
        Connection conn4 = hanaConnectionPool.getConnection();
        Connection conn5 = hanaConnectionPool.getConnection();

        // now we can use this pool the way we want.
        System.err.println("111 Are we connected? " + !conn1.isClosed());
        System.err.println("222 Are we connected? " + !conn2.isClosed());
        System.err.println("333 Are we connected? " + !conn3.isClosed());
        System.err.println("444 Are we connected? " + !conn4.isClosed());
        System.err.println("555 Are we connected? " + !conn5.isClosed());

        System.err.println("0 Idle Connections: " + OBJECT_POOL.getNumIdle() + ", out of " + OBJECT_POOL.getNumActive());
        hanaConnectionPool.returnConnection(conn1);
        System.err.println("1 Idle Connections: " + OBJECT_POOL.getNumIdle() + ", out of " + OBJECT_POOL.getNumActive());
        hanaConnectionPool.returnConnection(conn2);
        System.err.println("2 Idle Connections: " + OBJECT_POOL.getNumIdle() + ", out of " + OBJECT_POOL.getNumActive());
        hanaConnectionPool.returnConnection(conn3);
        System.err.println("3 Idle Connections: " + OBJECT_POOL.getNumIdle() + ", out of " + OBJECT_POOL.getNumActive());
        hanaConnectionPool.returnConnection(conn4);
        System.err.println("4 Idle Connections: " + OBJECT_POOL.getNumIdle() + ", out of " + OBJECT_POOL.getNumActive());
        hanaConnectionPool.returnConnection(conn5);
        System.err.println("5 Idle Connections: " + OBJECT_POOL.getNumIdle() + ", out of " + OBJECT_POOL.getNumActive());

    }


}
