package com.sap.shield.data;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/31/12
 * Time: 6:44 PM
 * To change this template use File | Settings | File Templates.
 */
public final class Int implements DataConverter {

    private Int() {
    }

    private static final Int INSTANCE = new Int();

    public static Int getInstance() {
        return INSTANCE;
    }

    public java.lang.String getXmlTypeName() {
        return "int";
    }

    public java.lang.String getDbTypeName() {
        return "INTEGER";
    }

    public byte[] toBytes(java.lang.String value) {
        return Bytes.toBytes(java.lang.Integer.parseInt(value));
    }

    public java.lang.String toString(byte[] bytes) {
        java.lang.String str = java.lang.Integer.toString(Bytes.toInt(bytes));
        if (StringUtils.isEmpty(str)) {
            str = "0";
        }
        return str;
    }

    public java.lang.String getNullString() {
        return "0";
    }
}
