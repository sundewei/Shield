package com.sap.shield.data;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.String;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/31/12
 * Time: 6:48 PM
 * To change this template use File | Settings | File Templates.
 */
public final class Short implements DataConverter {

    private Short() {
    }

    private static final Short INSTANCE = new Short();

    public static Short getInstance() {
        return INSTANCE;
    }

    public java.lang.String getXmlTypeName() {
        return "short";
    }

    public java.lang.String getDbTypeName() {
        return "TINYINT";
    }

    public byte[] toBytes(java.lang.String value) {
        return Bytes.toBytes(java.lang.Short.parseShort(value));
    }

    public java.lang.String toString(byte[] bytes) {
        java.lang.String str = java.lang.Short.toString(Bytes.toShort(bytes));
        if (StringUtils.isEmpty(str)) {
            str = "0";
        }
        return str;
    }

    public String getNullString() {
        return "0";
    }
}
