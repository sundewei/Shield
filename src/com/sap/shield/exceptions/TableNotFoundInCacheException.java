package com.sap.shield.exceptions;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/31/12
 * Time: 12:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class TableNotFoundInCacheException extends ShieldException {
    public TableNotFoundInCacheException(String msg) {
        super(msg);
    }

    public TableNotFoundInCacheException() {
        super();
    }
}
