package com.sap.shield.ext.logging;

import com.sap.shield.Constants;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 11/8/12
 * Time: 4:18 PM
 * To change this template use File | Settings | File Templates.
 */
public class ShieldLogFormatter extends Formatter {

    private static final Logger LOG = Logger.getLogger(ShieldLogFormatter.class.getName());
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss Z]");

    protected ShieldLogFormatter() {
        super();
    }

    @Override
    public String format(LogRecord record) {
        String loggerName = record.getLoggerName();
        long mills = record.getMillis();
        String levelName = record.getLevel().getName();
        String message = record.getMessage();
        StringBuilder csv = new StringBuilder();
        CSVPrinter csvPrinter = new CSVPrinter(csv, Constants.XS_ENGINE_CSV_FORMAT);
        try {
            csvPrinter.printRecord(loggerName, DATE_FORMAT.format(new Timestamp(mills)), levelName, message);
        } catch (IOException ioe) {
            LOG.log(Level.SEVERE, ioe.getMessage(), ioe);
        }
        return csv.toString();
    }
}
