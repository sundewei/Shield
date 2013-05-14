package com.sap.shield.resources;

import com.sap.shield.ActionHelper;
import com.sap.shield.DbHelper;
import com.sap.shield.cache.ResultScannerMap;
import com.sap.shield.cache.ShieldTableColumns;
import com.sap.shield.data.DataConverter;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.ServerResource;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 1/9/13
 * Time: 10:37 AM
 * To change this template use File | Settings | File Templates.
 */
public class DbLoadPreparer extends ServerResource {
    private static final Logger LOG = Logger.getLogger(DbLoadPreparer.class.getName());

    @org.restlet.resource.Get()
    public Representation get(Representation entity) {
        String resultScannerKey = (String) getRequestAttributes().get("resultScannerKey");
        java.lang.System.out.println("\n\n\n\nresultScannerKey=" + resultScannerKey);
        String tableName = (String) getRequestAttributes().get("tableName");
        java.lang.System.out.println("tableName=" + tableName);
        Representation representation = null;
        try {
            List<String> columnNames = ResultScannerMap.getInstance().getResultColumnNames(resultScannerKey);
            java.lang.System.out.println("columnNames=" + columnNames);
            int tableId = ResultScannerMap.getInstance().getTableId(resultScannerKey);
            java.lang.System.out.println("tableId=" + tableId);
            TreeSet<ShieldTableColumns.Row> rows = ShieldTableColumns.getRowByTableId(tableId);
            java.lang.System.out.println("rows=" + rows);
            List<DataConverter> converters = new ArrayList<DataConverter>();
            for (String columnName : columnNames) {
                converters.add(ActionHelper.getInstance().getDataConverter(rows, columnName));
            }
            DbHelper.getInstance().createLoadingTable(tableName, columnNames, converters);
            representation = new StringRepresentation("ok");
            this.setStatus(Status.SUCCESS_OK);
            java.lang.System.out.println("okkkkk");
        } catch (SQLException sqle) {
            this.setStatus(Status.SERVER_ERROR_INTERNAL, sqle.getMessage());
            LOG.log(Level.WARNING, sqle.getMessage(), sqle);
            ResultScannerMap.getInstance().close(resultScannerKey);
        }
        return representation;
    }
}
