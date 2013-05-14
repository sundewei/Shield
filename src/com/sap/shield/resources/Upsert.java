package com.sap.shield.resources;

import com.sap.shield.*;
import com.sap.shield.cache.ShieldColumns;
import com.sap.shield.cache.ShieldTableColumns;
import com.sap.shield.cache.ShieldTables;
import com.sap.shield.data.DataConverter;
import com.sap.shield.exceptions.HbaseShieldTableExistException;
import com.sap.shield.exceptions.ShieldException;
import com.sap.shield.ext.parsers.Parser;
import com.sap.shield.messages.SendRequest;
import com.sap.shield.messages.UpdateTableRequest;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Delete;
import org.restlet.resource.Post;
import org.restlet.resource.Put;
import org.restlet.resource.ServerResource;

import java.io.IOException;
import java.lang.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/30/12
 * Time: 2:38 PM
 * To change this template use File | Settings | File Templates.
 */
public class Upsert extends ServerResource {

    private static final Logger LOG = Logger.getLogger(Upsert.class.getName());

    @Post()
    public Representation post(Representation entity) {
        SendRequest sendRequest = null;
        Representation representation = null;
        try {
            if (!entity.getMediaType().toString().matches(MediaType.TEXT_CSV.toString())) {
                throw new ShieldException("MediaType not supported: " + entity.getMediaType().toString());
            }
            int tableId = Integer.parseInt((String) getRequestAttributes().get(Constants.TABLE_ID));
//java.lang.System.out.println("Got logs for table Id: " + tableId);
            sendRequest = getSendRequest(entity, tableId);
            sendRequest.addProperty(Constants.TABLE_ID, tableId);
            sendRequest.addProperty(Constants.TABLE_NAME, ShieldTables.getTableName(tableId));
            putMessages(sendRequest);
            representation = new StringRepresentation("ok");
        } catch (IOException ioe) {
//ioe.printStackTrace();
            this.setStatus(Status.SERVER_ERROR_INTERNAL, ioe.getMessage());
            LOG.log(Level.WARNING, ioe.getMessage(), ioe);
        } catch (SQLException sqle) {
//sqle.printStackTrace();
            this.setStatus(Status.SERVER_ERROR_INTERNAL, sqle.getMessage());
            LOG.log(Level.WARNING, sqle.getMessage(), sqle);
        } catch (ShieldException se) {
//se.getMessage();
            this.setStatus(Status.CLIENT_ERROR_BAD_REQUEST, se.getMessage());
            LOG.log(Level.WARNING, se.getMessage(), se);
        }
        return representation;
    }

    private SendRequest getSendRequest(Representation entity, int tableId) throws IOException, ShieldException {
        ShieldTables.Row table = ShieldTables.getRow(tableId);
        TreeSet<ShieldTableColumns.Row> rows = ShieldTableColumns.getRowByTableId(tableId);
        Parser parser = null;
        try {
            parser = ShieldManager.getInstance().getParser(table.getParserClassName());
        } catch (Exception e) {
            ShieldException se = new ShieldException(e.getMessage());
            se.setStackTrace(e.getStackTrace());
            throw se;
        }

        if (parser == null) {
            return ActionHelper.getInstance().getSendRequestFromCsv(entity.getText());
        } else {
            return ActionHelper.getInstance().getSendRequestFromParser(entity.getText(), parser, rows);
        }
    }

    @Put()
    public Representation put(Representation entity) {
        String movedUrl = ShieldManager.getInstance().getMovedUrl(getRequest().getHostRef().getPath());
        if (movedUrl != null) {
            this.setStatus(Status.REDIRECTION_TEMPORARY);
            return new StringRepresentation(movedUrl);
        }

        UpdateTableRequest request = null;
        Representation representation = null;
        try {
            int tableId = Integer.parseInt((String) getRequestAttributes().get(Constants.TABLE_ID));
            request = ActionHelper.getInstance().getUpdateTableRequestFromXml(entity.getText(), tableId);
            if (!ShieldTables.tableExist(tableId)) {
                throw new HbaseShieldTableExistException("Table '" + request.getPropertyInt(Constants.TABLE_ID) + "' does not exists.");
            } else {
                request.addProperty(Constants.TABLE_NAME, ShieldTables.getTableName(tableId));
            }
            ShieldManager.getInstance().updateShieldTable(request);
            ShieldManager.getInstance().addUpdateSequence();
            representation = new StringRepresentation("ok");
        } catch (IOException ioe) {
            this.setStatus(Status.SERVER_ERROR_INTERNAL, ioe.getMessage());
            LOG.log(Level.WARNING, ioe.getMessage(), ioe);
        } catch (SQLException sqle) {
            this.setStatus(Status.SERVER_ERROR_INTERNAL, sqle.getMessage());
            LOG.log(Level.WARNING, sqle.getMessage(), sqle);
        } catch (ShieldException se) {
            this.setStatus(Status.CLIENT_ERROR_BAD_REQUEST, se.getMessage());
            LOG.log(Level.INFO, se.getMessage(), se);
        }
        return representation;
    }

    @Delete()
    public Representation delete(Representation entity) {
        String movedUrl = ShieldManager.getInstance().getMovedUrl(getRequest().getHostRef().getPath());
        if (movedUrl != null) {
            this.setStatus(Status.REDIRECTION_TEMPORARY);
            return new StringRepresentation(movedUrl);
        }

        Representation representation = null;
        try {
            int tableId = Integer.parseInt((String) getRequestAttributes().get(Constants.TABLE_ID));
            ShieldManager.getInstance().deleteShieldTable(tableId);
            representation = new StringRepresentation("ok");
        } catch (ShieldException se) {
            se.printStackTrace();
            this.setStatus(Status.CLIENT_ERROR_BAD_REQUEST, se.getMessage());
            LOG.log(Level.INFO, se.getMessage(), se);
        } catch (SQLException se) {
            se.printStackTrace();
            this.setStatus(Status.SERVER_ERROR_INTERNAL, se.getMessage());
            LOG.log(Level.WARNING, se.getMessage(), se);
        } catch (IOException se) {
            se.printStackTrace();
            this.setStatus(Status.SERVER_ERROR_INTERNAL, se.getMessage());
            LOG.log(Level.WARNING, se.getMessage(), se);
        }
        return representation;
    }

    private void putMessages(SendRequest sendRequest) throws IOException, SQLException, ShieldException {
        HTableInterface hTable = HbaseHelper.getInstance().getHTable(sendRequest.getPropertyString(Constants.TABLE_NAME));
        List<List<String>> allValues = sendRequest.getAllValues();
        TreeSet<ShieldTableColumns.Row> rows = ShieldTableColumns.getRowByTableId(sendRequest.getPropertyInt(Constants.TABLE_ID));
        DataConverter[] dataConverters = CacheHelper.getInstance().getDataConverters(sendRequest.getPropertyInt(Constants.TABLE_ID));
        ShieldTables.Row table = ShieldTables.getRow(sendRequest.getPropertyInt(Constants.TABLE_ID));
        List<org.apache.hadoop.hbase.client.Put> puts = new ArrayList<org.apache.hadoop.hbase.client.Put>();
        if (sendRequest.isKeyValueRequest()) {
            org.apache.hadoop.hbase.client.Put put = new org.apache.hadoop.hbase.client.Put(HbaseHelper.getInstance().getRowKey(rows, sendRequest, table.appendTimestamp()));
            for (ShieldTableColumns.Row row: rows) {
                ShieldColumns.Row column = ShieldColumns.getRow(row.getColumnId());
                String key = column.getName();
                String value = sendRequest.getValue(key);
                if (value != null) {
//java.lang.System.out.println("\n\nkey="+key);
//java.lang.System.out.println("value="+value);
//java.lang.System.out.println("ActionHelper.getInstance().getDataConverter(rows, key)="+ActionHelper.getInstance().getDataConverter(rows, key));
                    put.add(Bytes.toBytes(Constants.DATA_COLUMN_FAMILY),
                            Bytes.toBytes(key),
                            ActionHelper.getInstance().getDataConverter(rows, key).toBytes(value));
                }
            }
            puts.add(put);
        } else {
            for (List<String> values : allValues) {
                org.apache.hadoop.hbase.client.Put put = new org.apache.hadoop.hbase.client.Put(HbaseHelper.getInstance().getRowKey(rows, values, table.appendTimestamp()));
                Iterator<ShieldTableColumns.Row> rowIterator = rows.iterator();
                for (int i = 0; i < values.size(); i++) {
                    DataConverter dataConverter = null;
                    if (i < dataConverters.length) {
                        dataConverter = dataConverters[i];
                    } else {
                        dataConverter = com.sap.shield.data.String.getInstance();
                    }
                    int columnId = rowIterator.next().getColumnId();
                    if (values.get(i) != null) {
                        put.add(Bytes.toBytes(Constants.DATA_COLUMN_FAMILY),
                                Bytes.toBytes(ShieldColumns.getRow(columnId).getName()),
                                dataConverter.toBytes(values.get(i)));
                    }
                }
                puts.add(put);
            }
        }
        hTable.put(puts);
        HbaseHelper.getInstance().returnHTableInterface(hTable);
    }
}
