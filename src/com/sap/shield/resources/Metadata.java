package com.sap.shield.resources;

import com.sap.shield.Constants;
import com.sap.shield.ShieldManager;
import com.sap.shield.TypeHelper;
import com.sap.shield.cache.ShieldColumns;
import com.sap.shield.cache.ShieldTableColumns;
import com.sap.shield.cache.ShieldTables;
import com.sap.shield.exceptions.CsvHandleException;
import com.sap.shield.exceptions.ShieldException;
import com.sap.shield.exceptions.TableNotFoundInCacheException;
import com.sap.shield.exceptions.UnrecognizedMetadataRequestException;
import org.apache.commons.csv.CSVPrinter;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

import java.io.IOException;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 11/8/12
 * Time: 9:27 PM
 * To change this template use File | Settings | File Templates.
 */
public class Metadata extends ServerResource {

    private static final Logger LOG = Logger.getLogger(Metadata.class.getName());

    @Get()
    public Representation get(Representation entity) {
        String movedUrl = ShieldManager.getInstance().getMovedUrl(getRequest().getHostRef().getPath());
        if (movedUrl != null) {
            this.setStatus(Status.REDIRECTION_TEMPORARY);
            return new StringRepresentation(movedUrl);
        }

        Representation representation = null;
        String path = getRequest().getOriginalRef().getPath();
        try {
            String returnTextValue = getValue(getRequest().getOriginalRef().getPath());
            if (returnTextValue != null) {
                representation = new StringRepresentation(returnTextValue);
            } else {
                throw new UnrecognizedMetadataRequestException("Unable to fetch metadata from path " + path);
            }
        } catch (ShieldException se) {
            se.printStackTrace();
            this.setStatus(Status.SERVER_ERROR_INTERNAL, se.getMessage());
            LOG.log(Level.INFO, se.getMessage(), se);
        }
        return representation;
    }

    private String getValue(String path) throws TableNotFoundInCacheException, CsvHandleException {
        String tableName = (String) getRequestAttributes().get(Constants.TABLE_NAME);
        String strTableId = (String) getRequestAttributes().get(Constants.TABLE_ID);
        if (path.contains("/metadata/tableId/")) {
            // Get tableId request
            return String.valueOf(ShieldTables.getTableId(tableName));
        } else if (path.contains("/metadata/tableName/")) {
            // Get tableName request
            return ShieldTables.getTableName(Integer.parseInt(strTableId));
        } else if (path.contains("/metadata/schema/")) {
            StringBuilder value = new StringBuilder();
            CSVPrinter csvPrinter = new CSVPrinter(value, Constants.XS_ENGINE_CSV_FORMAT);

            TreeSet<ShieldTableColumns.Row> rows = ShieldTableColumns.getRowByTableId(ShieldTables.getTableId(tableName));
            try {
                csvPrinter.printRecord("id", "name", "type", "sortOrder", "rowkeyIncludedOrder");
                for (ShieldTableColumns.Row row : rows) {
                    ShieldColumns.Row column = ShieldColumns.getRow(row.getColumnId());
                    csvPrinter.printRecord(
                            String.valueOf(row.getColumnId()),
                            column.getName(),
                            TypeHelper.getInstance().getDataConverter(column.getDataType()).getXmlTypeName(),
                            String.valueOf(row.getOrderNum()),
                            String.valueOf(row.getRowKeyOrder()));
                }
            } catch (IOException ioe) {
                CsvHandleException csvHandleException = new CsvHandleException(ioe.getMessage());
                csvHandleException.setStackTrace(ioe.getStackTrace());
                throw csvHandleException;
            }
            return value.toString();
        }
        return null;
    }
}
