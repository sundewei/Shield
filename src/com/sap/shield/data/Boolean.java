package com.sap.shield.data;


import org.apache.hadoop.hbase.util.Bytes;

import java.lang.String;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/31/12
 * Time: 6:46 PM
 * To change this template use File | Settings | File Templates.
 */
public final class Boolean implements DataConverter {
    private Boolean() {
    }

    private static final Boolean INSTANCE = new Boolean();

    public static Boolean getInstance() {
        return INSTANCE;
    }

    public java.lang.String getXmlTypeName() {
        return "boolean";
    }

    public java.lang.String getDbTypeName() {
        return "VARCHAR(5)";
    }

    public byte[] toBytes(java.lang.String value) {
        return Bytes.toBytes(java.lang.Boolean.parseBoolean(value));
    }

    public java.lang.String toString(byte[] bytes) {
        return java.lang.Boolean.toString(Bytes.toBoolean(bytes));
    }

    public String getNullString() {
        return "";
    }
}
