package com.sap.shield.exceptions;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/31/12
 * Time: 9:10 PM
 * To change this template use File | Settings | File Templates.
 */
public class InvalidColumnNameException extends ShieldException {
    public InvalidColumnNameException(String msg) {
        super(msg);
    }

    public InvalidColumnNameException() {
        super();
    }
}
