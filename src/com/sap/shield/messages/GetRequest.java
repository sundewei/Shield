package com.sap.shield.messages;

import com.sap.shield.Constants;
import com.sap.shield.exceptions.ShieldException;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 12/3/12
 * Time: 3:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class GetRequest extends BaseRequest {

    public void addSelectedColumn(String name) throws ShieldException {
        addStringToList(Constants.SELECTED_COLUMN_NAMES, name);
    }

    public void addRowkey(String rowkey) throws ShieldException {
        addStringToList(Constants.ROW_KEYS, rowkey);
    }

    public void addRowkeyDisplay(String name, boolean display) throws ShieldException {
        addBooleanToMap(Constants.ROW_KEY_DISPLAYS, name, display);
    }

    public boolean getRowkeyDisplay(String name) throws ShieldException {
        return getBooleanFromMap(Constants.ROW_KEY_DISPLAYS, name, false);
    }
}
