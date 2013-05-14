package com.sap.shield.resources;

import com.sap.shield.ActionHelper;
import com.sap.shield.Constants;
import com.sap.shield.ShieldManager;
import com.sap.shield.cache.ResultScannerMap;
import com.sap.shield.exceptions.ResultScannerExpiredException;
import com.sap.shield.exceptions.ShieldException;
import com.sap.shield.messages.LoopRequest;
import org.apache.hadoop.hbase.client.Result;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 11/7/12
 * Time: 9:37 PM
 * To change this template use File | Settings | File Templates.
 */
public class Loop extends ServerResource {

    private static final Logger LOG = Logger.getLogger(Loop.class.getName());

    @Get()
    public Representation get(Representation entity) {
        String movedUrl = ShieldManager.getInstance().getMovedUrl(getRequest().getHostRef().getPath());
        if (movedUrl != null) {
            this.setStatus(Status.REDIRECTION_TEMPORARY);
            return new StringRepresentation(movedUrl);
        }
        LoopRequest request = null;
        Representation representation = null;
        String resultScannerKey = (String) getRequestAttributes().get("resultScannerKey");
        List<String> columnNames = ResultScannerMap.getInstance().getResultColumnNames(resultScannerKey);
        try {
            if (!ResultScannerMap.getInstance().exist(resultScannerKey)) {
                throw new ResultScannerExpiredException("No ResultScanner or ResultScanner has exhausted for key: " + resultScannerKey);
            }
            int batch = Integer.parseInt((String) getRequestAttributes().get("batch"));
            request = new LoopRequest();
            request.addProperty(Constants.RESULT_SCANNER_KEY, resultScannerKey);
            request.addProperty(Constants.BATCH, batch);
            int tableId = ResultScannerMap.getInstance().getTableId(resultScannerKey);
//long startMs = java.lang.System.currentTimeMillis();
//java.lang.System.out.println("Before getting results");
            Result[] results = getResults(request);
//long endMs = java.lang.System.currentTimeMillis();
//java.lang.System.out.println("Getting results takes " + ((endMs-startMs)/1000));
            String resultCsv = ActionHelper.getInstance().getCsv(columnNames, results, tableId);
//long endCsvMs = java.lang.System.currentTimeMillis();
//java.lang.System.out.println("Convert results to CSV takes " + ((endCsvMs-endMs)/1000));
            representation = new StringRepresentation(resultCsv);
            representation.setSize(resultCsv.length());
            this.setStatus(Status.SUCCESS_OK);
        } catch (IOException ioe) {
            this.setStatus(Status.SERVER_ERROR_INTERNAL, ioe.getMessage());
            LOG.log(Level.WARNING, ioe.getMessage(), ioe);
            ResultScannerMap.getInstance().close(resultScannerKey);
        } catch (SQLException sqle) {
            this.setStatus(Status.SERVER_ERROR_INTERNAL, sqle.getMessage());
            LOG.log(Level.WARNING, sqle.getMessage(), sqle);
            ResultScannerMap.getInstance().close(resultScannerKey);
        } catch (ShieldException se) {
            this.setStatus(Status.CLIENT_ERROR_BAD_REQUEST, se.getMessage());
            LOG.log(Level.WARNING, se.getMessage(), se);
            ResultScannerMap.getInstance().close(resultScannerKey);
        }
        return representation;
    }

    private Result[] getResults(LoopRequest request) throws IOException, SQLException, ShieldException {
        String key = request.getPropertyString(Constants.RESULT_SCANNER_KEY);
        Result[] results = ResultScannerMap.getInstance().getResult(key, request.getPropertyInt(Constants.BATCH));
        if (results.length == 0 || results.length < request.getPropertyInt(Constants.BATCH)) {
            ResultScannerMap.getInstance().close(key);
        }
        return results;
    }
}
