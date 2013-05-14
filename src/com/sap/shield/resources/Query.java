package com.sap.shield.resources;

import com.sap.shield.*;
import com.sap.shield.cache.ResultScannerMap;
import com.sap.shield.cache.ShieldColumns;
import com.sap.shield.cache.ShieldTableColumns;
import com.sap.shield.cache.ShieldTables;
import com.sap.shield.data.DataConverter;
import com.sap.shield.data.Int;
import com.sap.shield.exceptions.ShieldException;
import com.sap.shield.messages.QueryRequest;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Post;
import org.restlet.resource.ServerResource;

import java.io.IOException;
import java.lang.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 11/6/12
 * Time: 2:35 PM
 * To change this template use File | Settings | File Templates.
 */
public class Query extends ServerResource {

    private static final Logger LOG = Logger.getLogger(Query.class.getName());

    @Post()
    public Representation post(Representation entity) {
        String movedUrl = ShieldManager.getInstance().getMovedUrl(getRequest().getHostRef().getPath());
        if (movedUrl != null) {
            this.setStatus(Status.REDIRECTION_TEMPORARY);
            return new StringRepresentation(movedUrl);
        }
        QueryRequest request = null;
        Representation representation = null;
        try {
            if (!entity.getMediaType().toString().matches(MediaType.APPLICATION_ALL_XML.toString())) {
                throw new ShieldException("MediaType not supported: " + entity.getMediaType().toString());
            }
            int tableId = Integer.parseInt((String) getRequestAttributes().get(Constants.TABLE_ID));
            Map<String, DataConverter> dataConverterMap = CacheHelper.getInstance().getDataConverterMap(tableId);
            request = ActionHelper.getInstance().getQueryRequest(entity.getText());
            request.addProperty(Constants.TABLE_ID, tableId);
            request.addProperty(Constants.TABLE_NAME, ShieldTables.getTableName(tableId));
            String key = getResultSannerKey(request, dataConverterMap);
            representation = new StringRepresentation(key);
            /*
Result[] results = ResultScannerMap.getInstance().getResult(key, 1000);
List<String> columnNames = ResultScannerMap.getInstance().getResultColumnNames(key);
String resultCsv = ActionHelper.getInstance().getCsv(columnNames, results, tableId);
java.lang.System.out.println("The result CSV= \n " + resultCsv);
            */

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

    private String getFirstRowkeyColumnName(TreeSet<ShieldTableColumns.Row> tableColumns) {
        for (ShieldTableColumns.Row row: tableColumns) {
            if (row.getRowKeyOrder() == 0) {
                return ShieldColumns.getRow(row.getColumnId()).getName();
            }
        }
        return null;
    }

    private String getResultSannerKey(QueryRequest request, Map<String, DataConverter> dataConverterMap) throws IOException, SQLException, ShieldException {
        List<String> names = request.getPropertyStringList(Constants.SELECTED_COLUMN_NAMES);
        List<String> regexes = request.getPropertyStringList(Constants.SELECTED_COLUMN_REGEX);
        List<String> maxs = request.getPropertyStringList(Constants.SELECTED_COLUMN_MAXS);
        List<String> mins = request.getPropertyStringList(Constants.SELECTED_COLUMN_MINS);
        int tableId = request.getPropertyInt(Constants.TABLE_ID);
        TreeSet<ShieldTableColumns.Row> tableColumns = ShieldTableColumns.getRowByTableId(tableId);
        String firstRowkey = getFirstRowkeyColumnName(tableColumns);
        HTableInterface htable = HbaseHelper.getInstance().getHTable(request.getPropertyString(Constants.TABLE_NAME));
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        Scan scan = null;
        byte[] startRow = getRow(firstRowkey, dataConverterMap, names, mins);
        byte[] endRow = getRow(firstRowkey, dataConverterMap, names, maxs);
//java.lang.System.out.println("Start From: " + Bytes.toString(startRow));
//java.lang.System.out.println("End   At  : " + Bytes.toString(endRow));
        if (startRow != null && endRow != null) {
            scan = new Scan(startRow, endRow);
        } else {
            scan = new Scan();
        }
//java.lang.System.out.println("\n\n");
        for (int i = 0; i < names.size(); i++) {
            Filter regFilter = getRegexFilter(names.get(i), regexes.get(i));
            // Adding the value filters
            DataConverter dataConverter = dataConverterMap.get(names.get(i));
            List<Filter> valueRangeFilters = getValueRangeFilter(names.get(i), dataConverter, maxs.get(i), mins.get(i));
            if (valueRangeFilters != null) {
                for (Filter f : valueRangeFilters) {
                    filterList.addFilter(f);
                }
            }
            if (regFilter != null) {
                filterList.addFilter(regFilter);
            }
//filterList.addFilter(new QualifierFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(names.get(i))));
            scan.addColumn(Bytes.toBytes(Constants.DATA_COLUMN_FAMILY), Bytes.toBytes(names.get(i)));
        }
        scan.setFilter(filterList);
        ResultScanner rs = htable.getScanner(scan);
        String rsKey = ResultScannerMap.getInstance().setResultScanner(request.getPropertyInt(Constants.TABLE_ID), htable, rs);
        ResultScannerMap.getInstance().setResultColumnNames(rsKey, names);
        return rsKey;
    }

    private byte[] getRow(String firstRowkey, Map<String, DataConverter> dataConverterMap, List<String> names, List<String> values) {
//java.lang.System.out.println("firstRowkey="+firstRowkey);
        for (int i = 0; i < names.size(); i++) {
//java.lang.System.out.println("names.get(i)="+names.get(i));
//java.lang.System.out.println("values.get(i)="+values.get(i));
            if (names.get(i).equals(firstRowkey) && !StringUtils.isEmpty(values.get(i))) {
                DataConverter dataConverter = dataConverterMap.get(names.get(i));
                if (dataConverter instanceof com.sap.shield.data.Int) {
                    try {
                        int value = Integer.parseInt(values.get(i));
                        return Bytes.toBytes(java.lang.String.valueOf(value));
                    } catch (NumberFormatException nfe) {
                        LOG.log(Level.WARNING, nfe.getMessage(), nfe);
                    }
                } else if (dataConverter instanceof com.sap.shield.data.Double) {
                    try {
                        double value = Double.parseDouble(values.get(i));
                        return Bytes.toBytes(java.lang.String.valueOf(value));
                    } catch (NumberFormatException nfe) {
                        LOG.log(Level.WARNING, nfe.getMessage(), nfe);
                    }
                } else if (dataConverter instanceof com.sap.shield.data.Long) {
                    try {
                        long value = Long.parseLong(values.get(i));
                        return Bytes.toBytes(java.lang.String.valueOf(value));
                    } catch (NumberFormatException nfe) {
                        LOG.log(Level.WARNING, nfe.getMessage(), nfe);
                    }
                } else if (dataConverter instanceof com.sap.shield.data.Short) {
                    try {
                        short value = Short.parseShort(values.get(i));
                        return Bytes.toBytes(java.lang.String.valueOf(value));
                    } catch (NumberFormatException nfe) {
                        LOG.log(Level.WARNING, nfe.getMessage(), nfe);
                    }
                }
            }
        }
        return null;
    }

    private static Filter getRegexFilter(String columnName, String regex) {
        if (!StringUtils.isEmpty(regex)) {
            RegexStringComparator comp = new RegexStringComparator(regex);
            return new SingleColumnValueFilter(Bytes.toBytes(Constants.DATA_COLUMN_FAMILY),
                    Bytes.toBytes(columnName),
                    CompareFilter.CompareOp.EQUAL,
                    comp);
        }
        return null;
    }

    private static List<Filter> getValueRangeFilter(String columnName, DataConverter dataConverter, String maxStr, String minStr) {
        if (dataConverter instanceof com.sap.shield.data.Int) {
            return getIntValueRangeFilter(columnName, maxStr, minStr);
        } else if (dataConverter instanceof com.sap.shield.data.Double) {
            return getDoubleValueRangeFilter(columnName, maxStr, minStr);
        } else if (dataConverter instanceof com.sap.shield.data.Long) {
            return getLongValueRangeFilter(columnName, maxStr, minStr);
        } else if (dataConverter instanceof com.sap.shield.data.Short) {
            return getShortValueRangeFilter(columnName, maxStr, minStr);
        }
        return null;
    }

    private static List<Filter> getIntValueRangeFilter(String columnName, String maxStr, String minStr) {
        List<Filter> filters = new ArrayList<Filter>();
        int max = Integer.MAX_VALUE;
        int min = Integer.MIN_VALUE;
        if (!StringUtils.isEmpty(maxStr) && StringUtils.isNumeric(maxStr)) {
            max = Integer.parseInt(maxStr);
            SingleColumnValueFilter maxFilter = new SingleColumnValueFilter(Bytes.toBytes(Constants.DATA_COLUMN_FAMILY),
                    Bytes.toBytes(columnName),
                    CompareFilter.CompareOp.LESS_OR_EQUAL,
                    Bytes.toBytes(max));
            maxFilter.setFilterIfMissing(true);
            filters.add(maxFilter);
        }

        if (!StringUtils.isEmpty(minStr) && StringUtils.isNumeric(minStr)) {
            min = Integer.parseInt(minStr);
            SingleColumnValueFilter minFilter = new SingleColumnValueFilter(Bytes.toBytes(Constants.DATA_COLUMN_FAMILY),
                    Bytes.toBytes(columnName),
                    CompareFilter.CompareOp.GREATER_OR_EQUAL,
                    Bytes.toBytes(min));
            minFilter.setFilterIfMissing(true);
            filters.add(minFilter);
        }
        return filters;
    }

    private static List<Filter> getDoubleValueRangeFilter(String columnName, String maxStr, String minStr) {
        List<Filter> filters = new ArrayList<Filter>();
        double max = Double.MAX_VALUE;
        double min = Double.MIN_VALUE;
        if (!StringUtils.isEmpty(maxStr) && StringUtils.isNumeric(maxStr)) {
            max = Double.parseDouble(maxStr);
            SingleColumnValueFilter maxFilter = new SingleColumnValueFilter(Bytes.toBytes(Constants.DATA_COLUMN_FAMILY),
                    Bytes.toBytes(columnName),
                    CompareFilter.CompareOp.LESS_OR_EQUAL,
                    Bytes.toBytes(max));
            maxFilter.setFilterIfMissing(true);
            filters.add(maxFilter);
        }

        if (!StringUtils.isEmpty(minStr) && StringUtils.isNumeric(minStr)) {
            min = Double.parseDouble(minStr);
            SingleColumnValueFilter minFilter = new SingleColumnValueFilter(Bytes.toBytes(Constants.DATA_COLUMN_FAMILY),
                    Bytes.toBytes(columnName),
                    CompareFilter.CompareOp.GREATER_OR_EQUAL,
                    Bytes.toBytes(min));
            minFilter.setFilterIfMissing(true);
            filters.add(minFilter);
        }
        return filters;
    }

    private static List<Filter> getLongValueRangeFilter(String columnName, String maxStr, String minStr) {
        List<Filter> filters = new ArrayList<Filter>();
        long max = Long.MAX_VALUE;
        long min = Long.MIN_VALUE;
        if (!StringUtils.isEmpty(maxStr) && StringUtils.isNumeric(maxStr)) {
            max = Long.parseLong(maxStr);
            SingleColumnValueFilter maxFilter = new SingleColumnValueFilter(Bytes.toBytes(Constants.DATA_COLUMN_FAMILY),
                    Bytes.toBytes(columnName),
                    CompareFilter.CompareOp.LESS_OR_EQUAL,
                    Bytes.toBytes(max));
            maxFilter.setFilterIfMissing(true);
            filters.add(maxFilter);
        }

        if (!StringUtils.isEmpty(minStr) && StringUtils.isNumeric(minStr)) {
            min = Long.parseLong(minStr);
            SingleColumnValueFilter minFilter = new SingleColumnValueFilter(Bytes.toBytes(Constants.DATA_COLUMN_FAMILY),
                    Bytes.toBytes(columnName),
                    CompareFilter.CompareOp.GREATER_OR_EQUAL,
                    Bytes.toBytes(min));
            minFilter.setFilterIfMissing(true);
            filters.add(minFilter);
        }
        return filters;
    }

    private static List<Filter> getShortValueRangeFilter(String columnName, String maxStr, String minStr) {
        List<Filter> filters = new ArrayList<Filter>();
        short max = Short.MAX_VALUE;
        short min = Short.MIN_VALUE;
        if (!StringUtils.isEmpty(maxStr) && StringUtils.isNumeric(maxStr)) {
            max = Short.parseShort(maxStr);
            SingleColumnValueFilter maxFilter = new SingleColumnValueFilter(Bytes.toBytes(Constants.DATA_COLUMN_FAMILY),
                    Bytes.toBytes(columnName),
                    CompareFilter.CompareOp.LESS_OR_EQUAL,
                    Bytes.toBytes(max));
            maxFilter.setFilterIfMissing(true);
            filters.add(maxFilter);
        }

        if (!StringUtils.isEmpty(minStr) && StringUtils.isNumeric(minStr)) {
            min = Short.parseShort(minStr);
            SingleColumnValueFilter minFilter = new SingleColumnValueFilter(Bytes.toBytes(Constants.DATA_COLUMN_FAMILY),
                    Bytes.toBytes(columnName),
                    CompareFilter.CompareOp.GREATER_OR_EQUAL,
                    Bytes.toBytes(min));
            minFilter.setFilterIfMissing(true);
            filters.add(minFilter);
        }
        return filters;
    }
}
