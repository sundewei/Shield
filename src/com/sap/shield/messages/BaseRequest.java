package com.sap.shield.messages;

import com.sap.shield.TypeHelper;
import com.sap.shield.exceptions.InvalidColumnNameException;
import com.sap.shield.exceptions.InvalidColumnTypeException;
import com.sap.shield.exceptions.ShieldException;
import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.util.*;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/26/12
 * Time: 11:16 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class BaseRequest {
    private static final Logger LOG = Logger.getLogger(BaseRequest.class.getName());

    private Map<String, Object> properties = new HashMap<String, Object>();

    private Map<String, Map<String, Boolean>> booleanMaps = new HashMap<String, Map<String, Boolean>>();

    private List<List<String>> values;

    public void addColumn(String name, String type, int rowKeyOrder) throws ShieldException {
        if (!TypeHelper.getInstance().isValidType(type)) {
            throw new InvalidColumnTypeException("Column: '" + name + "' has an invalid column type: " + type);
        }

        if (StringUtils.isEmpty(name)) {
            throw new InvalidColumnNameException("Column name cannot be an empty string or null");
        }

        if (StringUtils.isEmpty(type)) {
            throw new InvalidColumnTypeException("Column: '" + name + "' has a type of an empty string or null");
        }
        addStringToList("columnNames", name);
        addStringToList("columnTypes", TypeHelper.getInstance().getDataConverter(type).getDbTypeName());
        addIntegerToList("columnRowKeyOrder", rowKeyOrder);
        LOG.info("Adding Column: " + name + " with data type: " + type + ", the rowKeyOrder = " + rowKeyOrder);
    }

    public void addValues(String... values) {
        if (this.values == null) {
            this.values = new ArrayList<List<String>>();
        }
        this.values.add(Arrays.asList(values));
    }

    public List<List<String>> getAllValues() {
        return values;
    }

    public boolean getBooleanFromMap(String key, String name, boolean defaultValue) throws ShieldException {
        if (booleanMaps.containsKey(key) && booleanMaps.get(key).containsKey(name)) {
            return booleanMaps.get(key).get(name);
        } else {
            return defaultValue;
        }
    }

    public void addBooleanToMap(String key, String name, boolean value) {
        Map<String, Boolean> map = booleanMaps.get(key);
        if (map == null) {
            map = new HashMap<String, Boolean>();
            booleanMaps.put(key, map);
        }
        map.put(name, value);
    }

    public void addProperty(String name, Object value) {
        properties.put(name, value);
    }

    public int getPropertyInt(String name) throws ShieldException {
        if (properties.get(name) instanceof Integer) {
            return (Integer) properties.get(name);
        } else if (properties.get(name) instanceof String) {
            return Integer.parseInt((String) properties.get(name));
        } else {
            throw new ShieldException("Integer property not found when processing request: '" + name + "'");
        }
    }

    public String getPropertyString(String name) throws ShieldException {
        if (properties.get(name) instanceof Integer) {
            return String.valueOf((((Integer) properties.get(name)).intValue()));
        } else if (properties.get(name) instanceof String) {
            return (String) properties.get(name);
        } else {
            return null;
        }
    }

    public Boolean getPropertyBoolean(String name, boolean defaultValue) throws ShieldException {
        if (properties.get(name) instanceof Integer) {
            return ((Integer) properties.get(name)).intValue() == 1;
        } else if (properties.get(name) instanceof String) {
            return Boolean.parseBoolean((String) properties.get(name));
        } else if (properties.get(name) instanceof Boolean) {
            return (Boolean) properties.get(name);
        } else {
            return defaultValue;
        }
    }

    public Timestamp getPropertyTimestamp(String name) throws ShieldException {
        if (properties.containsKey(name)) {
            return (Timestamp) properties.get(name);
        } else {
            throw new ShieldException("Timestamp property not found when processing request: '" + name + "'");
        }
    }

    public List<String> getPropertyStringList(String name) throws ShieldException {
        return getPropertyStringList(name, true);
    }

    public List<String> getPropertyStringList(String name, boolean checkKey) throws ShieldException {
        if (properties.containsKey(name)) {
            return (List<String>) properties.get(name);
        } else {
            if (checkKey) {
                throw new ShieldException("String List property not found when processing request: '" + name + "'");
            }
        }
        return null;
    }

    public List<Integer> getPropertyIntegerList(String name) throws ShieldException {
        return getPropertyIntegerList(name, true);
    }

    public List<Integer> getPropertyIntegerList(String name, boolean checkKey) throws ShieldException {
        if (properties.containsKey(name)) {
            return (List<Integer>) properties.get(name);
        } else {
            if (checkKey) {
                throw new ShieldException("Integer List property not found when processing request: '" + name + "'");
            }
        }
        return null;
    }

    public List<Boolean> getPropertyBooleanList(String name, boolean checkKey) throws ShieldException {
        if (properties.containsKey(name)) {
            return (List<Boolean>) properties.get(name);
        } else {
            if (checkKey) {
                throw new ShieldException("Boolean List property not found when processing request: '" + name + "'");
            }
        }
        return null;
    }

    public int[] getPropertyIntArray(String name) throws ShieldException {
        if (properties.containsKey(name)) {
            return (int[]) properties.get(name);
        } else {
            throw new ShieldException("Int Array property not found when processing request: '" + name + "'");
        }
    }

    public void addStringToList(String propertyName, String value) throws ShieldException {
        List<String> list = getPropertyStringList(propertyName, false);
        if (list == null) {
            list = new ArrayList<String>();
        }
        list.add(value);
        properties.put(propertyName, list);
    }

    public void addBooleanToList(String propertyName, boolean value) throws ShieldException {
        List<Boolean> list = getPropertyBooleanList(propertyName, false);
        if (list == null) {
            list = new ArrayList<Boolean>();
        }
        list.add(value);
        properties.put(propertyName, list);
    }

    public void addIntegerToList(String propertyName, int value) throws ShieldException {
        List<Integer> list = getPropertyIntegerList(propertyName, false);
        if (list == null) {
            list = new ArrayList<Integer>();
        }
        list.add(value);
        properties.put(propertyName, list);
    }
}
