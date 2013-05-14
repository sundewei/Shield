package com.sap.shield;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.Quote;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/26/12
 * Time: 11:28 AM
 * To change this template use File | Settings | File Templates.
 */
public final class Constants {
    public static final String DATA_COLUMN_FAMILY = "cf";
    public static final String METADATA_COLUMN_FAMILY = "md";
    public static String HOSTNAME;
    public static String MASTER_URL;
    public static int MASTER_PORT;
    public static String MASTER_SCHEME;
    public static boolean MANAGER_NODE;
    public static final String ROW_KEY_DELIMITER = "~";
    //public static final String ROW_KEY_DELIMITER = "_";
    public static final int HTABLE_POOL_SIZE = 50;

    public static final String TABLE_ID = "tableId";
    public static final String TABLE_NAME = "tableName";
    public static final String APPEND_TIMESTAMP = "appendTimestamp";
    public static final String SELECTED_COLUMN_NAMES = "selectedColumnNames";
    public static final String SELECTED_COLUMN_REGEX = "selectedColumnRegex";
    public static final String SELECTED_COLUMN_MAXS = "max";
    public static final String SELECTED_COLUMN_MINS = "min";
    public static final String ROW_KEYS = "rowKeys";
    public static final String ROW_KEY_DISPLAYS = "rowKeyDisplays";
    public static final String RESULT_SCANNER_KEY = "resultScannerKey";
    public static final String BATCH = "batch";
    public static final Quote QUOTE_POLICY = Quote.ALL;
    public static final CSVFormat XS_ENGINE_CSV_FORMAT =
            CSVFormat.newBuilder().withDelimiter(',').withQuoteChar('\'').withQuotePolicy(QUOTE_POLICY).build();
}
