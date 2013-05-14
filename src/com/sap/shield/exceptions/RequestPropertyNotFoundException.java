package com.sap.shield.exceptions;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 12/5/12
 * Time: 3:17 PM
 * To change this template use File | Settings | File Templates.
 */
public class RequestPropertyNotFoundException extends ShieldException {
    public RequestPropertyNotFoundException(String msg) {
        super(msg);
    }

    public RequestPropertyNotFoundException() {
        super();
    }
}