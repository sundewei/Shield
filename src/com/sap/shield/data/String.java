package com.sap.shield.data;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/31/12
 * Time: 6:44 PM
 * To change this template use File | Settings | File Templates.
 */
public final class String implements DataConverter {

    private String() {
    }

    private static final String INSTANCE = new String();

    public static String getInstance() {
        return INSTANCE;
    }

    public java.lang.String getXmlTypeName() {
        return "string";
    }

    public java.lang.String getDbTypeName() {
        return "VARCHAR(5000)";
    }

    public byte[] toBytes(java.lang.String value) {
        return Bytes.toBytes(value);
    }

    public java.lang.String toString(byte[] bytes) {
        return Bytes.toString(bytes);
    }

    public java.lang.String getNullString() {
        return "NULL";
    }
}
