package com.sap.shield;

import com.sap.hadoop.conf.ConfigurationManager;
import com.sap.shield.cache.ShieldColumns;
import com.sap.shield.cache.ShieldTableColumns;
import com.sap.shield.exceptions.HbaseConnectingException;
import com.sap.shield.messages.SendRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/25/12
 * Time: 2:56 PM
 * To change this template use File | Settings | File Templates.
 */
public final class HbaseHelper {

    public HBaseAdmin hbaseAdmin;

    private static final Logger LOG = Logger.getLogger(HbaseHelper.class.getName());

    // Singleton
    private static final HbaseHelper INSTANCE = new HbaseHelper();

    private HTablePool hTablePool;

    private HbaseHelper() {
        try {
            init();
        } catch (Exception e) {
            HbaseConnectingException ee = new HbaseConnectingException(e.getMessage());
            ee.setStackTrace(e.getStackTrace());
            LOG.log(Level.SEVERE, ee.getMessage(), ee);
        }
    }

    public static HbaseHelper getInstance() {
        return INSTANCE;
    }

    private ConfigurationManager configurationManager;
    private Configuration hbaseConfiguration;

    private void init() throws Exception {
        configurationManager = new ConfigurationManager("hadoop", "abcd1234");
        hbaseConfiguration = HBaseConfiguration.create(configurationManager.getConfiguration());
        hbaseAdmin = new HBaseAdmin(hbaseConfiguration);
        hTablePool = new HTablePool(hbaseConfiguration, 500);
    }

    public synchronized void createTable(String tableName, List<String> columnFamilies) throws IOException {
        if (columnFamilies.size() == 0) {
            throw new IOException("Column family count cannot be 0.");
        }

        // Get a descriptor of the table first
        HTableDescriptor desc = getHTableDescriptor(tableName, columnFamilies);

        if (hbaseAdmin.tableExists(tableName)) {
            throw new IOException("Table '" + tableName + "' already existed.");
        }

        // now create the table
        if (!hbaseAdmin.tableExists(tableName)) {
            hbaseAdmin.createTable(desc);
        }
    }

    private HTableDescriptor getHTableDescriptor(String tableName, Collection<String> columnFamilies) {
        // Get a descriptor of the table first
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);

        // Add a column family of wanted columns to the descriptor
        for (String columnFamilie : columnFamilies) {
            HColumnDescriptor colDesc = new HColumnDescriptor(Bytes.toBytes(columnFamilie));
            hTableDescriptor.addFamily(colDesc);
        }

        return hTableDescriptor;
    }

    public HTableInterface getHTable(String tableName) throws IOException {
        if (hbaseAdmin.tableExists(tableName)) {
            // return a reference
            return hTablePool.getTable(tableName);
        } else {
            return null;
        }
    }

    public void returnHTableInterface(HTableInterface hTableInterface) throws IOException {
        hTablePool.putTable(hTableInterface);
    }

    public boolean tableExists(String tableName) throws IOException {
        return hbaseAdmin.tableExists(tableName);
    }

    public synchronized void disableTable(String tableName) throws IOException {
        hbaseAdmin.disableTable(tableName);
    }

    public synchronized void dropTable(String tableName) throws IOException {
        sleepQuietly(1000);
        if (!hbaseAdmin.isTableDisabled(tableName)) {
            hbaseAdmin.disableTable(tableName);
        }
        hbaseAdmin.deleteTable(tableName);
    }

    public void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            // do nothing
        }
    }

    public byte[] getRowKey(TreeSet<ShieldTableColumns.Row> rows, SendRequest sendRequest, boolean appendTs) {
        TreeSet<RowKeyCalculator> rowKeyCalculators = new TreeSet<RowKeyCalculator>();
        int i = 0;
        for (ShieldTableColumns.Row row : rows) {
            if (row.getRowKeyOrder() >= 0) {
                RowKeyCalculator rowKeyCalculator = new RowKeyCalculator(sendRequest.getValue(ShieldColumns.getRow(row.getColumnId()).getName()), row.getRowKeyOrder());
                rowKeyCalculators.add(rowKeyCalculator);
            }
            i++;
        }
        StringBuilder key = new StringBuilder();
        for (RowKeyCalculator rowKeyCalculator : rowKeyCalculators) {
            key.append(rowKeyCalculator.value).append(com.sap.shield.Constants.ROW_KEY_DELIMITER);
        }
        if (appendTs) {
            key.append(String.valueOf(getReverseMilliSecond()));
        } else {
            key.deleteCharAt(key.length() - 1);
        }
System.out.println("Rowkey='" + key.toString()+"'");
        return Bytes.toBytes(key.toString());
    }

    public byte[] getRowKey(TreeSet<ShieldTableColumns.Row> rows, List<String> values, boolean appendTs) {
        TreeSet<RowKeyCalculator> rowKeyCalculators = new TreeSet<RowKeyCalculator>();
        int i = 0;
        for (ShieldTableColumns.Row row : rows) {
            if (row.getRowKeyOrder() >= 0) {
                RowKeyCalculator rowKeyCalculator = new RowKeyCalculator(values.get(i), row.getRowKeyOrder());
                rowKeyCalculators.add(rowKeyCalculator);
            }
            i++;
        }
        StringBuilder key = new StringBuilder();
        for (RowKeyCalculator rowKeyCalculator : rowKeyCalculators) {
            key.append(rowKeyCalculator.value).append(com.sap.shield.Constants.ROW_KEY_DELIMITER);
        }
        if (appendTs) {
            key.append(String.valueOf(getReverseMilliSecond()));
        } else {
            key.deleteCharAt(key.length() - 1);
        }
//System.out.println("Rowkey='" + key.toString()+"'");
        return Bytes.toBytes(key.toString());
    }

    public long getReverseMilliSecond() {
        return Long.MAX_VALUE - System.currentTimeMillis();
    }

    public static class RowKeyCalculator implements Comparable<RowKeyCalculator> {
        private String value;
        private int rowKeyOrder;

        public RowKeyCalculator(String value, int rowKeyOrder) {
            this.value = value;
            this.rowKeyOrder = rowKeyOrder;
        }

        public int compareTo(RowKeyCalculator o) {
            return this.rowKeyOrder - o.rowKeyOrder;
        }
    }
}
