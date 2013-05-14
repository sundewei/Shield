package com.sap.shield.data;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.String;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/31/12
 * Time: 6:45 PM
 * To change this template use File | Settings | File Templates.
 */
public final class Long implements DataConverter {

    private Long() {
    }

    private static final Long INSTANCE = new Long();

    public static Long getInstance() {
        return INSTANCE;
    }

    public java.lang.String getXmlTypeName() {
        return "long";
    }

    public java.lang.String getDbTypeName() {
        return "BIGINT";
    }

    public byte[] toBytes(java.lang.String value) {
        return Bytes.toBytes(java.lang.Long.parseLong(value));
    }

    public java.lang.String toString(byte[] bytes) {
        java.lang.String str = java.lang.Long.toString(Bytes.toLong(bytes));
        if (StringUtils.isEmpty(str)) {
            str = "0";
        }
        return str;
    }

    public String getNullString() {
        return "0";
    }

}
