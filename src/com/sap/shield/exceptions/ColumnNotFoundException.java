package com.sap.shield.exceptions;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 12/2/12
 * Time: 6:20 PM
 * To change this template use File | Settings | File Templates.
 */
public class ColumnNotFoundException extends ShieldException {
    public ColumnNotFoundException(String msg) {
        super(msg);
    }

    public ColumnNotFoundException() {
        super();
    }
}