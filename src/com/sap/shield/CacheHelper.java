package com.sap.shield;

import com.sap.shield.cache.ShieldColumns;
import com.sap.shield.cache.ShieldTableColumns;
import com.sap.shield.cache.ShieldTables;
import com.sap.shield.data.DataConverter;
import com.sap.shield.exceptions.ShieldException;
import com.sap.shield.messages.CreateTableRequest;
import com.sap.shield.messages.UpdateTableRequest;

import java.sql.Timestamp;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/30/12
 * Time: 4:52 PM
 * To change this template use File | Settings | File Templates.
 */
public final class CacheHelper {
    // Singleton
    private static final CacheHelper INSTANCE = new CacheHelper();

    private CacheHelper() {
    }

    public static CacheHelper getInstance() {
        return INSTANCE;
    }

    private Map<Integer, DataConverter[]> dataConverterMap = new HashMap<Integer, DataConverter[]>();

    public void reloadDataConverter(int tableId) {
        dataConverterMap.remove(tableId);
        dataConverterMap.put(tableId, getDataConverters(tableId));
    }

    public DataConverter[] getDataConverters(int tableId) {
        if (dataConverterMap.containsKey(tableId)) {
            return dataConverterMap.get(tableId);
        }
        TreeSet<ShieldTableColumns.Row> rows = ShieldTableColumns.getRowByTableId(tableId);
        DataConverter[] dataConverters = new DataConverter[rows.size()];
        Iterator<ShieldTableColumns.Row> iterator = rows.iterator();
        for (int i = 0; i < dataConverters.length; i++) {
            dataConverters[i] = TypeHelper.getInstance().getDataConverter(ShieldColumns.getRow(iterator.next().getColumnId()).getDataType());
        }
        dataConverterMap.put(tableId, dataConverters);
        return dataConverters;
    }

    public Map<String, DataConverter> getDataConverterMap(int tableId) {
        TreeSet<ShieldTableColumns.Row> rows = ShieldTableColumns.getRowByTableId(tableId);
        Map<String, DataConverter> map = new HashMap<String, DataConverter>();
        Iterator<ShieldTableColumns.Row> iterator = rows.iterator();
        for (int i = 0; i < rows.size(); i++) {
            int columnId = iterator.next().getColumnId();
            map.put(ShieldColumns.getRow(columnId).getName(), TypeHelper.getInstance().getDataConverter(ShieldColumns.getRow(columnId).getDataType()));
        }
        return map;
    }

    public void deleteTable(int tableId) {
        if (ShieldTables.tableExist(tableId)) {
            ShieldTables.DATA.remove(tableId);
        }
        TreeSet<ShieldTableColumns.Row> rows = ShieldTableColumns.getRowByTableId(tableId);
        for (ShieldTableColumns.Row row : rows) {
            ShieldColumns.deleteColumn(row.getColumnId());
        }
        ShieldTableColumns.deleteRowsByTableId(tableId);
        if (ShieldTables.DATA.size() == 0) {
            ShieldTables.setMaxId(7);
        }
    }

    public void createTable(CreateTableRequest request) throws ShieldException {
        // upsert the event tables in cache
        int nextTableId = request.getPropertyInt("maxTableId") + 15;
        Timestamp timestamp = request.getPropertyTimestamp("timestamp");
        ShieldTables.Row etRow = new ShieldTables.Row(nextTableId, request.getPropertyString(Constants.TABLE_NAME), request.getPropertyString("parserClassName"), request.getPropertyBoolean(Constants.APPEND_TIMESTAMP, true), timestamp);
        ShieldTables.upsertRow(etRow);
        request.addProperty(Constants.TABLE_ID, nextTableId);

        // upsert the event columns in cache
        ShieldColumns.Row[] ecRows = getShieldColumnRows(request.getPropertyInt(Constants.TABLE_ID), request.getPropertyStringList("columnNames"), request.getPropertyStringList("columnTypes"));
        ShieldColumns.upsertRows(ecRows);
        int[] columnIds = getColumnIds(ecRows);
        request.addProperty("columnIds", columnIds);

        // upsert the event table-column in cache
        int[] orderNums = getOrderNums(ecRows.length, 15, 10);
        ShieldTableColumns.Row[] etcRows = getShieldTableColumnRows(etRow.getId(), columnIds, orderNums, request.getPropertyIntegerList("columnRowKeyOrder"));
//System.out.println("\n\n\nAbout to upsert tableColumn rows: " + etRow.getId() + ", etcRows.length = " + etcRows.length);
        ShieldTableColumns.upsertRows(etcRows);
        request.addProperty("orderNums", orderNums);
    }

    public void deleteTableColumns(int id) {
        TreeSet<ShieldTableColumns.Row> rows = ShieldTableColumns.getRowByTableId(id);
        for (ShieldTableColumns.Row row : rows) {
            ShieldColumns.deleteColumn(row.getColumnId());
        }
        ShieldTableColumns.deleteRowsByTableId(id);
    }

    private int[] getColumnIds(TreeSet<ShieldTableColumns.Row> oldRows) {
        int[] cids = new int[oldRows.size()];
        int i = 0;
        for (ShieldTableColumns.Row row : oldRows) {
            cids[i] = row.getColumnId();
            i++;
        }
        return cids;
    }

    public void updateShieldTable(UpdateTableRequest request) throws ShieldException {
        TreeSet<ShieldTableColumns.Row> oldRows = ShieldTableColumns.getRowByTableId(request.getPropertyInt(Constants.TABLE_ID));

        request.addProperty("oldColumnIds", getColumnIds(oldRows));

        deleteTableColumns(request.getPropertyInt(Constants.TABLE_ID));

        // upsert the event columns in cache
        ShieldColumns.Row[] ecRows = getShieldColumnRows(ShieldColumns.getMaxId(), request.getPropertyStringList("columnNames"), request.getPropertyStringList("columnTypes"));
        ShieldColumns.upsertRows(ecRows);
        int[] columnIds = getColumnIds(ecRows);
        request.addProperty("columnIds", columnIds);

        // upsert the event table-column in cache
        int[] orderNums = getOrderNums(ecRows.length, 15, 10);
        ShieldTableColumns.Row[] etcRows = getShieldTableColumnRows(request.getPropertyInt(Constants.TABLE_ID), columnIds, orderNums, request.getPropertyIntegerList("columnRowKeyOrder"));
        ShieldTableColumns.upsertRows(etcRows);
        request.addProperty("orderNums", orderNums);
    }

    private ShieldColumns.Row[] getShieldColumnRows(int startingId, List<String> columnNames, List<String> columnDataTypes) {
        ShieldColumns.Row[] rows = new ShieldColumns.Row[columnNames.size()];
        for (int i = 0; i < rows.length; i++) {
            startingId += 10;
            rows[i] = new ShieldColumns.Row(startingId, columnNames.get(i), columnDataTypes.get(i));
        }
        return rows;
    }

    private int[] getColumnIds(ShieldColumns.Row[] rows) {
        int[] cids = new int[rows.length];
        for (int i = 0; i < rows.length; i++) {
            cids[i] = rows[i].getId();
        }
        return cids;
    }

    private int[] getOrderNums(int count, int start, int width) {
        int[] arr = new int[count];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = start + (width * i);
        }
        return arr;
    }

    private ShieldTableColumns.Row[] getShieldTableColumnRows(int tableId, int[] cids, int[] orderNums, List<Integer> rowKeyOrder) {
        ShieldTableColumns.Row[] rows = new ShieldTableColumns.Row[cids.length];
        for (int i = 0; i < cids.length; i++) {
            rows[i] = new ShieldTableColumns.Row(tableId, cids[i], orderNums[i], rowKeyOrder.get(i));
        }
        return rows;
    }


}
