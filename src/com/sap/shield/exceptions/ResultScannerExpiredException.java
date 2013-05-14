package com.sap.shield.exceptions;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 11/7/12
 * Time: 8:49 PM
 * To change this template use File | Settings | File Templates.
 */
public class ResultScannerExpiredException extends ShieldException {
    public ResultScannerExpiredException(String msg) {
        super(msg);
    }

    public ResultScannerExpiredException() {
        super();
    }
}
