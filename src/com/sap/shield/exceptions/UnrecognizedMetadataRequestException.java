package com.sap.shield.exceptions;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 11/9/12
 * Time: 12:24 AM
 * To change this template use File | Settings | File Templates.
 */
public class UnrecognizedMetadataRequestException extends ShieldException {
    public UnrecognizedMetadataRequestException(String msg) {
        super(msg);
    }

    public UnrecognizedMetadataRequestException() {
        super();
    }
}