package com.sap.shield.data;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/31/12
 * Time: 5:48 PM
 * To change this template use File | Settings | File Templates.
 */
public interface DataConverter {
    public java.lang.String getXmlTypeName();

    public java.lang.String getDbTypeName();

    public byte[] toBytes(java.lang.String value);

    public java.lang.String toString(byte[] bytes);

    public java.lang.String getNullString();
}
