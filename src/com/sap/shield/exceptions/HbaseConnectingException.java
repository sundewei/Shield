package com.sap.shield.exceptions;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/25/12
 * Time: 3:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class HbaseConnectingException extends ShieldException {
    public HbaseConnectingException(String msg) {
        super(msg);
    }

    public HbaseConnectingException() {
        super();
    }
}
