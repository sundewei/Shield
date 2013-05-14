package com.sap.shield.cache;


import com.sap.shield.HbaseHelper;
import com.sap.shield.exceptions.ResultScannerExpiredException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 11/7/12
 * Time: 8:15 PM
 * To change this template use File | Settings | File Templates.
 */
public final class ResultScannerMap {
    private static final Logger LOG = Logger.getLogger(ResultScannerMap.class.getName());

    public static ResultScannerMap INSTANCE = new ResultScannerMap();

    private ResultScannerMap() {
    }

    public static ResultScannerMap getInstance() {
        return INSTANCE;
    }

    private Map<String, ResultScanner> resultScannerMap = new HashMap<String, ResultScanner>();
    private Map<String, HTableInterface> resultScannerHtableMap = new HashMap<String, HTableInterface>();
    private Map<String, Timestamp> resultScannerAccessedMap = new HashMap<String, Timestamp>();
    private Map<String, Integer> keyTableIdMap = new HashMap<String, Integer>();
    private Map<String, List<String>> resultColumnNames = new HashMap<String, List<String>>();

    public String setResultScanner(int tableId, HTableInterface htable, ResultScanner resultScanner) {
        UUID uuid = UUID.randomUUID();
        String randomUUIDString = uuid.toString();
        resultScannerAccessedMap.put(randomUUIDString, new Timestamp(System.currentTimeMillis()));
        resultScannerHtableMap.put(randomUUIDString, htable);
        resultScannerMap.put(randomUUIDString, resultScanner);
        keyTableIdMap.put(randomUUIDString, tableId);
//System.out.println("resultScannerAccessedMap.containsKey("+randomUUIDString+")="+resultScannerAccessedMap.containsKey(randomUUIDString));
//System.out.println("resultScannerHtableMap.containsKey("+randomUUIDString+")="+resultScannerHtableMap.containsKey(randomUUIDString));
//System.out.println("resultScannerMap.containsKey("+randomUUIDString+")="+resultScannerMap.containsKey(randomUUIDString));
//System.out.println("keyTableIdMap.containsKey("+randomUUIDString+")="+keyTableIdMap.containsKey(randomUUIDString));
        return randomUUIDString;
    }

    public boolean exist(String key) {
        return resultScannerMap.containsKey(key);
    }

    public void close(String key) {
//System.out.println("\n\n\nClosing the key from the maps..." + key);
        keyTableIdMap.remove(key);
        resultScannerAccessedMap.remove(key);
        if (resultScannerHtableMap.containsKey(key)) {
            try {
                HbaseHelper.getInstance().returnHTableInterface(resultScannerHtableMap.remove(key));
            } catch (IOException e) {
                LOG.log(Level.WARNING, e.getMessage());
            }
        }
        if (resultScannerMap.containsKey(key)) {
            resultScannerMap.remove(key).close();
        }

        if (resultColumnNames.containsKey(key)) {
            resultColumnNames.remove(key);
        }

    }

    public Integer getTableId(String key) {
        return keyTableIdMap.get(key);
    }

    public Result[] getResult(String key, int batch) throws ResultScannerExpiredException {
        if (resultScannerMap.containsKey(key) && resultScannerHtableMap.containsKey(key)) {
            ResultScanner rs = resultScannerMap.get(key);
            try {
                Result[] results = rs.next(batch);
                return results;
            } catch (IOException e) {
                LOG.log(Level.WARNING, e.getMessage());
            }
        }
        throw new ResultScannerExpiredException("The ResultScanner for key: '" + key + "' has expired and removed");
    }

    public void setResultColumnNames(String key, List<String> columnNames) {
        resultColumnNames.put(key, columnNames);
    }

    public List<String> getResultColumnNames(String key) {
        return resultColumnNames.get(key);
    }
}
