package com.sap.shield;

import com.sap.shield.cache.ShieldColumns;
import com.sap.shield.cache.ShieldTableColumns;
import com.sap.shield.cache.ShieldTables;
import com.sap.shield.data.DataConverter;
import com.sap.shield.exceptions.ShieldException;
import com.sap.shield.ext.parsers.Parser;
import com.sap.shield.ext.parsers.ValueFeeder;
import com.sap.shield.messages.*;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.lang.String;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/30/12
 * Time: 3:18 PM
 * To change this template use File | Settings | File Templates.
 */
public final class ActionHelper {

    private static final Logger LOG = Logger.getLogger(ActionHelper.class.getName());

    private static final ActionHelper INSTANCE = new ActionHelper();
    private DateFormat dateFormat0 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
    private DateFormat dateFormat1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
    private DateFormat dateFormat2 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm");
    private DateFormat dateFormat3 = new SimpleDateFormat("dd/MMM/yyyy:HH");
    private DateFormat dateFormat4 = new SimpleDateFormat("dd/MMM/yyyy");
    private List<DateFormat> dateFormats = new ArrayList<DateFormat>();

    private ActionHelper() {
        dateFormats.add(dateFormat0);
        dateFormats.add(dateFormat1);
        dateFormats.add(dateFormat2);
        dateFormats.add(dateFormat3);
        dateFormats.add(dateFormat4);
    }

    public static ActionHelper getInstance() {
        return INSTANCE;
    }

    public CreateTableRequest getCreateTableRequestFromXml(String xmlText) throws IOException, ShieldException {
        CreateTableRequest request = new CreateTableRequest();
        try {
            Document document = MessageHelper.DOCUMENT_BUILDER.parse(new InputSource(new StringReader(xmlText)));
            Node tableNode = document.getElementsByTagName("table").item(0);
            NodeList columnNodes = document.getElementsByTagName("column");
            request.addProperty(Constants.TABLE_NAME, tableNode.getAttributes().getNamedItem("name").getTextContent());
            NamedNodeMap tableAtts = tableNode.getAttributes();
            if (tableAtts.getNamedItem("parserClass") != null) {
                request.addProperty("parserClassName", tableAtts.getNamedItem("parserClass").getTextContent());
            }

            if (tableAtts.getNamedItem("appendTimestamp") != null) {
                request.addProperty(Constants.APPEND_TIMESTAMP, Boolean.parseBoolean(tableAtts.getNamedItem(Constants.APPEND_TIMESTAMP).getTextContent()));
            }

            for (int i = 0; i < columnNodes.getLength(); i++) {
                NamedNodeMap attMap = columnNodes.item(i).getAttributes();
                int rowKeyOrder = -1;
                if (attMap.getNamedItem("rowKeyOrder") != null) {
                    rowKeyOrder = Integer.parseInt(attMap.getNamedItem("rowKeyOrder").getTextContent());
                }
                request.addColumn(attMap.getNamedItem("name").getTextContent(), attMap.getNamedItem("type").getTextContent(), rowKeyOrder);
            }
            return request;
        } catch (SAXException se) {
            LOG.warning(se.getMessage());
            IOException ioe = new IOException(se.getMessage());
            ioe.setStackTrace(se.getStackTrace());
            throw ioe;
        }
    }

    public UpdateTableRequest getUpdateTableRequestFromXml(String xmlText, int tableId) throws IOException, ShieldException {
        UpdateTableRequest request = new UpdateTableRequest();
        try {
            Document document = MessageHelper.DOCUMENT_BUILDER.parse(new InputSource(new StringReader(xmlText)));
            NodeList columnNodes = document.getElementsByTagName("column");
            request.addProperty(Constants.TABLE_ID, tableId);
            request.addProperty(Constants.TABLE_NAME, ShieldTables.getTableName(tableId));
            for (int i = 0; i < columnNodes.getLength(); i++) {
                NamedNodeMap attMap = columnNodes.item(i).getAttributes();
                int rowKeyOrder = -1;
                if (attMap.getNamedItem("rowKeyOrder") != null) {
                    rowKeyOrder = Integer.parseInt(attMap.getNamedItem("rowKeyOrder").getTextContent());
                }
                request.addColumn(attMap.getNamedItem("name").getTextContent(), attMap.getNamedItem("type").getTextContent(), rowKeyOrder);
            }
            return request;
        } catch (SAXException se) {
            LOG.warning(se.getMessage());
            IOException ioe = new IOException(se.getMessage());
            ioe.setStackTrace(se.getStackTrace());
            throw ioe;
        }
    }

    private String[] getStringArray(CSVRecord csvRecord) {
        String[] array = new String[csvRecord.size()];
        for (int i = 0; i < csvRecord.size(); i++) {
            array[i] = csvRecord.get(i);
        }
        return array;
    }

    public SendRequest getSendRequestFromCsv(String csvText) throws IOException {
        SendRequest request = new SendRequest();
        BufferedReader reader = new BufferedReader(new StringReader(csvText));
        CSVParser csvParser = new CSVParser(reader, Constants.XS_ENGINE_CSV_FORMAT);
        List<CSVRecord> csvRecords = csvParser.getRecords();
        boolean kvReq = isKeyValues(csvRecords.get(0));
        request.setKeyValueRequest(kvReq);
        for (CSVRecord csvRecord : csvRecords) {
            if (kvReq) {
                for (String pair: csvRecord) {
                    String[] kv = pair.split("=");
                    request.setKeyValue(kv[0], kv[1]);
                }
            } else {
                request.addValues(getStringArray(csvRecord));
            }
        }
        return request;
    }

    private boolean isKeyValues(CSVRecord csvRecord) {
        for (int i = 0; i < csvRecord.size(); i++) {
            if (csvRecord.get(i).indexOf("=") < 0) {
                return false;
            }
        }
        return true;
    }

    public SendRequest getSendRequestFromParser(String text, Parser parser, TreeSet<ShieldTableColumns.Row> rows) throws IOException {
        SendRequest request = new SendRequest();
        BufferedReader reader = new BufferedReader(new StringReader(text));
        String line = reader.readLine();
        while (line != null) {
            ValueFeeder valueFeeder = parser.getValueFeeder(line);
            if (valueFeeder != null) {
                String[] values = new String[rows.size()];
                int i = 0;
                for (ShieldTableColumns.Row row : rows) {
                    values[i] = valueFeeder.getValue(ShieldColumns.getRow(row.getColumnId()).getName());
                    i++;
                }
                request.addValues(values);
            }
            line = reader.readLine();
        }
        return request;
    }

    public GetRequest getGetRequest(String xmlText) throws IOException, ShieldException {
        GetRequest request = new GetRequest();
        try {
            Document document = MessageHelper.DOCUMENT_BUILDER.parse(new InputSource(new StringReader(xmlText)));
            Node shieldNode = document.getElementsByTagName("shield").item(0);
            NodeList nodeList = shieldNode.getChildNodes();
            int len = nodeList.getLength();
            // Deal with the query that uses rowkey to fetch
            for (int i = 0; i < len; i++) {
                Node node = nodeList.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    if ("rowkey".equalsIgnoreCase(node.getNodeName())) {
                        //System.out.println("RowKey requested:" + node.getAttributes().getNamedItem("value").getTextContent());
                        request.addRowkey(node.getAttributes().getNamedItem("value").getTextContent());
                        request.addRowkeyDisplay(node.getAttributes().getNamedItem("value").getTextContent(), Boolean.getBoolean(node.getAttributes().getNamedItem("display").getTextContent()));
                    } else if ("column".equalsIgnoreCase(node.getNodeName())) {
                        //System.out.println("Column requested:" + node.getAttributes().getNamedItem("name").getTextContent());
                        request.addSelectedColumn(node.getAttributes().getNamedItem("name").getTextContent());
                    }
                }
            }
        } catch (SAXException se) {
            LOG.warning(se.getMessage());
            IOException ioe = new IOException(se.getMessage());
            ioe.setStackTrace(se.getStackTrace());
            throw ioe;
        }
        return request;
    }

    public QueryRequest getQueryRequest(String xmlText) throws IOException, ShieldException {
        QueryRequest request = new QueryRequest();
        try {
            Document document = MessageHelper.DOCUMENT_BUILDER.parse(new InputSource(new StringReader(xmlText)));
            Node shieldNode = document.getElementsByTagName("shield").item(0);
            NodeList nodeList = shieldNode.getChildNodes();
            int len = nodeList.getLength();
            for (int i = 0; i < len; i++) {
                Node node = nodeList.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE &&
                        !"rowkey".equalsIgnoreCase(node.getNodeName()) &&
                        !"column".equalsIgnoreCase(node.getNodeName())) {
                    NamedNodeMap attMap = node.getAttributes();
                    String regex = attMap.getNamedItem("regex") != null ? attMap.getNamedItem("regex").getTextContent() : "";
                    String maxStr = attMap.getNamedItem("max") != null ? attMap.getNamedItem("max").getTextContent() : "";
                    String minStr = attMap.getNamedItem("min") != null ? attMap.getNamedItem("min").getTextContent() : "";
                    if (StringUtils.isEmpty(minStr) && attMap.getNamedItem("minDate") != null) {
                        String minDateStr = attMap.getNamedItem("minDate").getTextContent();
//System.out.println("Found minDateStr = " + minDateStr);
                        try {
                            minStr = String.valueOf(getPossibleMs(minDateStr));
                        } catch (ParseException pe) {
                            LOG.log(Level.WARNING, pe.getMessage(), pe);
                            minStr = "";
                        }
                    }

                    if (StringUtils.isEmpty(maxStr) && attMap.getNamedItem("maxDate") != null) {
                        String maxDateStr = attMap.getNamedItem("maxDate").getTextContent();
//System.out.println("Found maxDateStr = " + maxDateStr);
                        try {
                            maxStr = String.valueOf(getPossibleMs(maxDateStr));
                        } catch (ParseException pe) {
                            LOG.log(Level.WARNING, pe.getMessage(), pe);
                            maxStr = "";
                        }
                    }
//System.out.println(node.getNodeName() + ": " + minStr + " ~ " + maxStr + ", regex -----> " + regex + "\n\n\n");
                    request.addStringToList(Constants.SELECTED_COLUMN_MAXS, maxStr);
                    request.addStringToList(Constants.SELECTED_COLUMN_MINS, minStr);
                    request.addStringToList(Constants.SELECTED_COLUMN_NAMES, node.getNodeName());
                    request.addStringToList(Constants.SELECTED_COLUMN_REGEX, regex);
                }
            }
        } catch (SAXException se) {
            LOG.warning(se.getMessage());
            IOException ioe = new IOException(se.getMessage());
            ioe.setStackTrace(se.getStackTrace());
            throw ioe;
        }
        return request;
    }

    private long getPossibleMs(String dateString) throws ParseException {
        Timestamp ts = null;
        for (DateFormat dateFormat: dateFormats) {
            try {
                ts = new Timestamp(dateFormat.parse(dateString).getTime());
            } catch (Exception e) {
                ts = null;
            }
            if (ts != null) {
                return ts.getTime();
            }
        }
        throw new ParseException("Unable to parse '" + dateString + "' against all date formats", 0);
    }

    public String getCsv(List<String> columnNames, Result[] rsArray, int tableId) throws IOException {
        TreeSet<ShieldTableColumns.Row> rows = ShieldTableColumns.getRowByTableId(tableId);
        StringBuilder csv = new StringBuilder();
        CSVPrinter csvPrinter = new CSVPrinter(csv, Constants.XS_ENGINE_CSV_FORMAT);
        for (int i = 0; i < rsArray.length; i++) {
            Result result = rsArray[i];
            List<String> values = new ArrayList<String>();
            for (String columnName : columnNames) {
                KeyValue keyValue = result.getColumnLatest(Bytes.toBytes(Constants.DATA_COLUMN_FAMILY), Bytes.toBytes(columnName));
                DataConverter converter = getDataConverter(rows, columnName);
                if (keyValue != null) {
//System.out.println(columnName + "---> converter.toString(keyValue.getValue())="+converter.toString(keyValue.getValue()));
                    values.add(converter.toString(keyValue.getValue()));
                } else {
//System.out.println(columnName + "---> converter.getNullString()="+converter.getNullString());
                    values.add(converter.getNullString());
                }
            }
            csvPrinter.printRecord(values);
        }
//System.out.println("------------------->" + csv.toString() + "<----------------------");
        return csv.toString();
    }

    public DataConverter getDataConverter(TreeSet<ShieldTableColumns.Row> rows, String name) {
        DataConverter dataConverter = com.sap.shield.data.String.getInstance();
        for (ShieldTableColumns.Row row : rows) {
            ShieldColumns.Row columnRow = ShieldColumns.getRow(row.getColumnId());
            if (columnRow.getName().equals(name)) {
                dataConverter = TypeHelper.getInstance().getDataConverter(columnRow.getDataType());
                return dataConverter;
            }
        }
        return dataConverter;
    }

    public static void main(String[] arg) throws Exception {
        ActionHelper.getInstance().getSendRequestFromCsv("'systemId=xxsee_111','timestamp=1361351205426','upTimeMs=1645605426','gallon=7567499','cubicFeet=0'");
    }

}
