package com.sap.shield.resources;

import com.sap.shield.*;
import com.sap.shield.cache.ShieldColumns;
import com.sap.shield.cache.ShieldTableColumns;
import com.sap.shield.cache.ShieldTables;
import com.sap.shield.exceptions.ShieldException;
import com.sap.shield.messages.GetRequest;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Post;
import org.restlet.resource.ServerResource;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 12/3/12
 * Time: 2:58 PM
 * To change this template use File | Settings | File Templates.
 */
public class Get extends ServerResource {

    private static final Logger LOG = Logger.getLogger(Query.class.getName());

    @Post()
    public Representation post(Representation entity) {
        String movedUrl = ShieldManager.getInstance().getMovedUrl(getRequest().getHostRef().getPath());
        if (movedUrl != null) {
            this.setStatus(Status.REDIRECTION_TEMPORARY);
            return new StringRepresentation(movedUrl);
        }

        GetRequest request = null;
        Representation representation = null;
        try {
            if (!entity.getMediaType().toString().matches(MediaType.APPLICATION_ALL_XML.toString())) {
                throw new ShieldException("MediaType not supported: " + entity.getMediaType().toString());
            }
            int tableId = Integer.parseInt((String) getRequestAttributes().get(Constants.TABLE_ID));
            request = ActionHelper.getInstance().getGetRequest(entity.getText());
            request.addProperty(Constants.TABLE_ID, tableId);
            request.addProperty(Constants.TABLE_NAME, ShieldTables.getTableName(tableId));
//java.lang.System.out.println("request.isGetQuery()="+request.isGetQuery());
            representation = new StringRepresentation(getResult(request));
        } catch (IOException ioe) {
            this.setStatus(Status.SERVER_ERROR_INTERNAL, ioe.getMessage());
            LOG.log(Level.WARNING, ioe.getMessage(), ioe);
        } catch (SQLException sqle) {
            this.setStatus(Status.SERVER_ERROR_INTERNAL, sqle.getMessage());
            LOG.log(Level.WARNING, sqle.getMessage(), sqle);
        } catch (ShieldException se) {
            this.setStatus(Status.CLIENT_ERROR_BAD_REQUEST, se.getMessage());
            LOG.log(Level.WARNING, se.getMessage(), se);
        }
        return representation;
    }

    private String getResult(GetRequest request) throws IOException, SQLException, ShieldException {
        List<String> names = request.getPropertyStringList(Constants.SELECTED_COLUMN_NAMES, false);
        HTableInterface htable = HbaseHelper.getInstance().getHTable(request.getPropertyString(Constants.TABLE_NAME));

        List<org.apache.hadoop.hbase.client.Get> gets = new ArrayList<org.apache.hadoop.hbase.client.Get>();
        List<String> selectedColumns = new ArrayList<String>();
        Map<String, Integer> selectCids = new HashMap<String, Integer>();
        // When rowkey is present
        for (String rowKey : request.getPropertyStringList(Constants.ROW_KEYS)) {
            java.lang.System.out.println("rowKey=" + rowKey);
            org.apache.hadoop.hbase.client.Get get = new org.apache.hadoop.hbase.client.Get(Bytes.toBytes(rowKey));
            if (names.size() > 0) {
                for (int i = 0; i < names.size(); i++) {
                    get.addColumn(Bytes.toBytes(Constants.DATA_COLUMN_FAMILY), Bytes.toBytes(names.get(i)));
                    selectedColumns.add(names.get(i));
                    selectCids.put(names.get(i), ShieldTableColumns.getRowId(request.getPropertyInt(Constants.TABLE_ID), names.get(i)));
                }
            } else {
                // If no column specified, then get all columns
                TreeSet<ShieldTableColumns.Row> rows = ShieldTableColumns.getRowByTableId(request.getPropertyInt(Constants.TABLE_ID));
                for (ShieldTableColumns.Row row : rows) {
                    String columnName = ShieldColumns.getRow(row.getColumnId()).getName();
                    get.addColumn(Bytes.toBytes(Constants.DATA_COLUMN_FAMILY), Bytes.toBytes(columnName));
                    selectedColumns.add(ShieldColumns.getRow(row.getColumnId()).getName());
                    selectCids.put(columnName, row.getColumnId());
                }
            }
            gets.add(get);
        }
        Result[] results = htable.get(gets);
        StringBuilder sb = new StringBuilder();
        CSVPrinter csvPrinter = new CSVPrinter(sb, Constants.XS_ENGINE_CSV_FORMAT);
        for (Result result : results) {
            List<String> values = new ArrayList<String>();
            String rowKey = Bytes.toString(result.getRow());
            if (request.getRowkeyDisplay(rowKey)) {
                values.add(rowKey);
            }
            for (String column : selectedColumns) {
                byte[] bytes = result.getValue(Bytes.toBytes(Constants.DATA_COLUMN_FAMILY), Bytes.toBytes(column));
                String strValue = TypeHelper.getInstance().getDataConverter(ShieldColumns.getRow(selectCids.get(column)).getDataType()).toString(bytes);
                values.add(strValue);
            }
            csvPrinter.printRecord(values);
        }
        HbaseHelper.getInstance().returnHTableInterface(htable);
        return sb.toString();
    }
}
