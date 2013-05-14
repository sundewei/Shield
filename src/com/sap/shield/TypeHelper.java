package com.sap.shield;

import com.sap.shield.data.Boolean;
import com.sap.shield.data.*;
import com.sap.shield.data.Double;
import com.sap.shield.data.Long;
import com.sap.shield.data.Short;
import com.sap.shield.data.String;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/31/12
 * Time: 8:48 PM
 * To change this template use File | Settings | File Templates.
 */
public final class TypeHelper {

    private static final Logger LOG = Logger.getLogger(TypeHelper.class.getName());

    private static final TypeHelper INSTANCE = new TypeHelper();

    private TypeHelper() {
    }

    public static TypeHelper getInstance() {
        return INSTANCE;
    }

    private static final Map<java.lang.String, DataConverter> XML_TYPE_LOOKUP =
            new HashMap<java.lang.String, DataConverter>();
    private static final Map<java.lang.String, DataConverter> DB_TYPE_LOOKUP =
            new HashMap<java.lang.String, DataConverter>();

    static {
        XML_TYPE_LOOKUP.put(com.sap.shield.data.Boolean.getInstance().getXmlTypeName(), Boolean.getInstance());
        DB_TYPE_LOOKUP.put(Boolean.getInstance().getDbTypeName(), Boolean.getInstance());

        XML_TYPE_LOOKUP.put(Double.getInstance().getXmlTypeName(), Double.getInstance());
        DB_TYPE_LOOKUP.put(Double.getInstance().getDbTypeName(), Double.getInstance());

        XML_TYPE_LOOKUP.put(Int.getInstance().getXmlTypeName(), Int.getInstance());
        DB_TYPE_LOOKUP.put(Int.getInstance().getDbTypeName(), Int.getInstance());

        XML_TYPE_LOOKUP.put(Long.getInstance().getXmlTypeName(), Long.getInstance());
        DB_TYPE_LOOKUP.put(Long.getInstance().getDbTypeName(), Long.getInstance());

        XML_TYPE_LOOKUP.put(Short.getInstance().getXmlTypeName(), Short.getInstance());
        DB_TYPE_LOOKUP.put(Short.getInstance().getDbTypeName(), Short.getInstance());

        XML_TYPE_LOOKUP.put(String.getInstance().getXmlTypeName(), String.getInstance());
        DB_TYPE_LOOKUP.put(String.getInstance().getDbTypeName(), String.getInstance());
    }

    public DataConverter getDataConverter(java.lang.String type) {
        if (XML_TYPE_LOOKUP.containsKey(type)) {
            return XML_TYPE_LOOKUP.get(type);
        } else {
            return DB_TYPE_LOOKUP.get(type);
        }
    }

    public boolean isValidType(java.lang.String xmlType) {
        return XML_TYPE_LOOKUP.containsKey(xmlType);
    }
}
