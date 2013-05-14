package com.sap.shield.cache;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/30/12
 * Time: 4:03 PM
 * To change this template use File | Settings | File Templates.
 */
public final class ShieldColumns {

    private static int MAX_ID = 3;

    public static Map<Integer, Row> DATA = new HashMap<Integer, Row>();

    public static void upsertRow(Row row) {
        DATA.put(row.id, row);
        if (MAX_ID < row.id) {
            MAX_ID = row.id;
        }
    }

    public static void deleteColumn(int cid) {
        DATA.remove(cid);
    }

    public static int getMaxId() {
        return MAX_ID;
    }

    public static void upsertRows(Row[] rows) {
        for (ShieldColumns.Row row : rows) {
            upsertRow(row);
        }
    }

    public static Row getRow(int id) {
        if (DATA.containsKey(id)) {
            return DATA.get(id).getCopy();
        } else {
            return null;
        }
    }

    public static class Row {
        private int id;
        private String name;
        private String dataType;

        public Row(int id, String name, String dataType) {
            this.id = id;
            this.name = name;
            this.dataType = dataType;
        }

        public Row getCopy() {
            return new Row(this.id, this.name, this.dataType);
        }

        public int getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public String getDataType() {
            return dataType;
        }

        @Override
        public String toString() {
            return "Row{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", dataType='" + dataType + '\'' +
                    '}';
        }
    }
}
