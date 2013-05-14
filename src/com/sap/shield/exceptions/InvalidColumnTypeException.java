package com.sap.shield.exceptions;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/31/12
 * Time: 9:06 PM
 * To change this template use File | Settings | File Templates.
 */
public class InvalidColumnTypeException extends ShieldException {
    public InvalidColumnTypeException(String msg) {
        super(msg);
    }

    public InvalidColumnTypeException() {
        super();
    }
}