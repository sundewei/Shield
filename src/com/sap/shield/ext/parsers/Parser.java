package com.sap.shield.ext.parsers;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 11/7/12
 * Time: 10:45 AM
 * To change this template use File | Settings | File Templates.
 */
public interface Parser {
    public ValueFeeder getValueFeeder(String line);
}
