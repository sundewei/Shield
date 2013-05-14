package com.sap.shield.cache;

import com.sap.shield.exceptions.ColumnNotFoundException;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/30/12
 * Time: 4:03 PM
 * To change this template use File | Settings | File Templates.
 */
public final class ShieldTableColumns {
    public static Map<Integer, TreeSet<Row>> DATA_BY_TABLE_ID = new HashMap<Integer, TreeSet<Row>>();
    private static Map<Integer, Row> DATA_BY_COLUMN_ID = new HashMap<Integer, Row>();

    public static void upsertRow(Row row) {
        TreeSet<Row> byTableRows = DATA_BY_TABLE_ID.get(row.tableId);
        if (byTableRows == null) {
            byTableRows = new TreeSet<Row>();
        }
        if (!byTableRows.contains(row)) {
            byTableRows.add(row);
        }
//System.out.println("byTableRows.size()===>"+byTableRows.size());
        DATA_BY_TABLE_ID.put(row.tableId, byTableRows);
        DATA_BY_COLUMN_ID.put(row.columnId, row);
    }

    public static void upsertRows(Row[] rows) {
        for (Row row : rows) {
            upsertRow(row);
        }
    }

    public static TreeSet<Row> getRowByTableId(int id) {
        if (DATA_BY_TABLE_ID.containsKey(id)) {
            return DATA_BY_TABLE_ID.get(id);
        } else {
            return null;
        }
    }

    public static void deleteRowsByTableId(int id) {
        TreeSet<Row> rows = DATA_BY_TABLE_ID.get(id);
        for (Row row : rows) {
            DATA_BY_COLUMN_ID.remove(row.columnId);
        }
        DATA_BY_TABLE_ID.remove(id);
    }

    public static Row getRowByColumnId(int id) {
        if (DATA_BY_COLUMN_ID.containsKey(id)) {
            return DATA_BY_COLUMN_ID.get(id).getCopy();
        } else {
            return null;
        }
    }

    public static int getRowId(int tableId, String columnName) throws ColumnNotFoundException {
        TreeSet<Row> rows = DATA_BY_TABLE_ID.get(tableId);
        for (Row row : rows) {
            if (ShieldColumns.getRow(row.getColumnId()).getName().equalsIgnoreCase(columnName)) {
                return row.getColumnId();
            }
        }
        throw new ColumnNotFoundException("No column found for tableId: " + tableId + " with column name ='" + columnName + "'");
    }

    public static class Row implements Comparable<Row> {
        private int tableId;
        private int columnId;
        private int orderNum;
        private int rowKeyOrder;

        public Row(int tableId, int columnId, int orderNum, int rowKeyOrder) {
            this.tableId = tableId;
            this.columnId = columnId;
            this.orderNum = orderNum;
            this.rowKeyOrder = rowKeyOrder;
        }

        public Row getCopy() {
            return new Row(tableId, columnId, orderNum, rowKeyOrder);
        }

        public int getTableId() {
            return tableId;
        }

        public int getColumnId() {
            return columnId;
        }

        public int getOrderNum() {
            return orderNum;
        }

        public int getRowKeyOrder() {
            return rowKeyOrder;
        }

        public int compareTo(Row o) {
            if (o.tableId == tableId && o.columnId == columnId) {
                return 0;
            } else {
                if (o.orderNum > orderNum) {
                    return -1;
                } else {
                    return 1;
                }
            }
        }

        @Override
        public String toString() {
            return "Row{" +
                    "tableId=" + tableId +
                    ", columnId=" + columnId +
                    ", orderNum=" + orderNum +
                    ", rowKeyOrder=" + rowKeyOrder +
                    '}';
        }
    }
}
