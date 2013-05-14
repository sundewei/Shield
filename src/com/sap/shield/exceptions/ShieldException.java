package com.sap.shield.exceptions;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/30/12
 * Time: 2:43 PM
 * To change this template use File | Settings | File Templates.
 */
public class ShieldException extends Exception {
    public ShieldException(String msg) {
        super(msg);
    }

    public ShieldException() {
        super();
    }
}
