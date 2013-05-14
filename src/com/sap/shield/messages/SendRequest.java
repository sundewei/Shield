package com.sap.shield.messages;

import java.lang.String;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/30/12
 * Time: 3:01 PM
 * To change this template use File | Settings | File Templates.
 */
public class SendRequest extends BaseRequest {
    private Map<String, String> keyValues;
    private boolean keyValueRequest;
    public void setKeyValueRequest(boolean keyValueRequest) {
        this.keyValueRequest = keyValueRequest;
        if (this.keyValueRequest) {
            keyValues = new HashMap<String, String>();
        }
    }

    public boolean isKeyValueRequest() {
        return keyValueRequest;
    }

    public void setKeyValue(String k, String v) {
        keyValues.put(k, v);
    }

    public String getValue(String key) {
        return keyValues.get(key);
    }
}
