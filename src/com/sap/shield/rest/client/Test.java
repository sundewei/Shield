package com.sap.shield.rest.client;

import com.sap.hadoop.conf.ConfigurationManager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 11/8/12
 * Time: 3:20 PM
 * To change this template use File | Settings | File Templates.
 */
public class Test {
    private static String DRIVER_NAME = "org.apache.hadoop.hive.jdbc.HiveDriver";

    public static void main(String[] arg) throws Exception {
        /*
        try {
            Class.forName(DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        */
        ConfigurationManager configurationManager = new ConfigurationManager("hadoop", "abcd1234");
        Connection connection = configurationManager.getConnection();
        Statement stmt = connection.createStatement();
        String sql = " show tables ";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1));
        }
        connection.close();
    }

}
