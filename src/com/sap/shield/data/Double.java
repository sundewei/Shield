package com.sap.shield.data;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/31/12
 * Time: 6:45 PM
 * To change this template use File | Settings | File Templates.
 */
public final class Double implements DataConverter {

    private Double() {
    }

    private static final Double INSTANCE = new Double();

    public static Double getInstance() {
        return INSTANCE;
    }

    public java.lang.String getXmlTypeName() {
        return "double";
    }

    public java.lang.String getDbTypeName() {
        return "DOUBLE";
    }

    public byte[] toBytes(java.lang.String value) {
        return Bytes.toBytes(java.lang.Double.parseDouble(value));
    }

    public java.lang.String toString(byte[] bytes) {
        java.lang.String str = java.lang.Double.toString(Bytes.toDouble(bytes));
        if (StringUtils.isEmpty(str)) {
            str = "0";
        }
        return str;
    }

    public java.lang.String getNullString() {
        return "0";
    }
}
