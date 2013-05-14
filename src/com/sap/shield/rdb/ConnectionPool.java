package com.sap.shield.rdb;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/29/12
 * Time: 4:39 PM
 * To change this template use File | Settings | File Templates.
 */
public interface ConnectionPool {

    public Connection getConnection() throws SQLException;

    public void returnConnection(Connection conn);
}
