package com.sap.shield.ext.parsers;

import java.sql.Timestamp;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/18/11
 * Time: 4:05 PM
 * To change this template use File | Settings | File Templates.
 */
public class AccessEntry implements ValueFeeder {

    public String ip;
    public Timestamp timestamp;
    public String method;
    public String resource;
    public int httpCode;
    public long dataLength;
    public String referrer;
    public String userAgent;
    public String yyyy;
    public String mm;
    public String dd;
    public String hh;

    public String getValue(String key) {
        if ("ip".equalsIgnoreCase(key)) {
            return ip;
        } else if ("timestamp".equalsIgnoreCase(key)) {
            return String.valueOf(timestamp.getTime());
        } else if ("method".equalsIgnoreCase(key)) {
            return method;
        } else if ("resource".equalsIgnoreCase(key)) {
            return resource;
        } else if ("httpCode".equalsIgnoreCase(key)) {
            return String.valueOf(httpCode);
        } else if ("dataLength".equalsIgnoreCase(key)) {
            return String.valueOf(dataLength);
        } else if ("referrer".equalsIgnoreCase(key)) {
            return String.valueOf(referrer);
        } else if ("userAgent".equalsIgnoreCase(key)) {
            return String.valueOf(userAgent);
        } else if ("yyyy".equalsIgnoreCase(key)) {
            return yyyy;
        } else if ("mm".equalsIgnoreCase(key)) {
            return mm;
        } else if ("dd".equalsIgnoreCase(key)) {
            return dd;
        } else if ("hh".equalsIgnoreCase(key)) {
            return hh;
        }
        return null;
    }
}
