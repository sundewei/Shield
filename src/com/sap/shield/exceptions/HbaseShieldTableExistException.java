package com.sap.shield.exceptions;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/30/12
 * Time: 2:41 PM
 * To change this template use File | Settings | File Templates.
 */
public class HbaseShieldTableExistException extends ShieldException {
    public HbaseShieldTableExistException(String msg) {
        super(msg);
    }

    public HbaseShieldTableExistException() {
        super();
    }
}