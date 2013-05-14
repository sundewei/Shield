package com.sap.shield.cache;

import com.sap.shield.exceptions.TableNotFoundInCacheException;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/30/12
 * Time: 4:02 PM
 * To change this template use File | Settings | File Templates.
 */
public final class ShieldTables {

    private static int MAX_ID = 7;

    public static Map<Integer, Row> DATA = new HashMap<Integer, Row>();

    public static void setMaxId(int mid) {
        MAX_ID = mid;
    }

    public static void upsertRow(Row row) {
        DATA.put(row.id, row);
        if (MAX_ID < row.id) {
            MAX_ID = row.id;
        }
    }

    public static int getMaxId() {
        return MAX_ID;
    }

    public static Row getRow(int id) {
        if (DATA.containsKey(id)) {
            return DATA.get(id).getCopy();
        } else {
            return null;
        }
    }

    public static boolean tableExist(int tableId) {
        return DATA.containsKey(tableId);
    }

    public static class Row {
        private int id;
        private String name;
        private Timestamp timestamp;
        private String parserClassName;
        private boolean appendTimestamp = true;

        public Row(int id, String name, String parserClassName, boolean appendTimestamp, Timestamp timestamp) {
            this.id = id;
            this.name = name;
            this.timestamp = timestamp;
            this.parserClassName = parserClassName;
            this.appendTimestamp = appendTimestamp;
        }

        public Row getCopy() {
            return new Row(this.id, this.name, this.parserClassName, this.appendTimestamp, this.timestamp);
        }

        public int getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public Timestamp getTimestamp() {
            return timestamp;
        }

        public String getParserClassName() {
            return parserClassName;
        }

        public boolean appendTimestamp() {
            return appendTimestamp;
        }

        public void setAppendTimestamp(boolean appendTimestamp) {
            this.appendTimestamp = appendTimestamp;
        }

        @Override
        public String toString() {
            return "Row{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", timestamp=" + timestamp +
                    ", parserClassName='" + parserClassName + '\'' +
                    ", appendTimestamp=" + appendTimestamp +
                    '}';
        }
    }

    public static String getTableName(int tableId) throws TableNotFoundInCacheException {
        if (!DATA.containsKey(tableId)) {
            throw new TableNotFoundInCacheException("Cannot find table with id: " + tableId);
        } else {
            return DATA.get(tableId).getName();
        }
    }

    public static int getTableId(String tableName) throws TableNotFoundInCacheException {
        for (Map.Entry<Integer, Row> entry : DATA.entrySet()) {
            if (entry.getValue().getName().equals(tableName)) {
                return entry.getValue().getId();
            }
        }
        throw new TableNotFoundInCacheException("Cannot find table with name = '" + tableName + "'");
    }
}
