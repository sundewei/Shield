package com.sap.shield.ext.parsers;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 11/6/12
 * Time: 11:28 PM
 * To change this template use File | Settings | File Templates.
 */
public class AccessLogParser implements Parser {
    private final DateFormat DATE_FORMAT = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss Z]");

    public static final CSVFormat SPACE_CSV_FORMAT = CSVFormat.newBuilder().withDelimiter(' ').build();
    public static final CSVFormat COMMA_CSV_FORMAT = CSVFormat.newBuilder().withDelimiter(',').build();

    public AccessLogParser() {
    }

    public ValueFeeder getValueFeeder(String line) {
        if (StringUtils.isEmpty(line)) {
            return null;
        }

        // An AccessData object will be created for each line if possible
        AccessEntry accessEntry = null;
        try {
            accessEntry = new AccessEntry();
            // Parse the value separated line using space as the delimiter
            CSVParser spaceCsvParser = new CSVParser(new StringReader(line), SPACE_CSV_FORMAT);

            // Now get all the values from the line
            CSVRecord csvRecord = spaceCsvParser.getRecords().get(0);

            // Get the yyyy, mm, dd and IP
            CSVParser commaCsvParser = new CSVParser(new StringReader(csvRecord.get(0)), COMMA_CSV_FORMAT);
            CSVRecord subValues = commaCsvParser.getRecords().get(0);
            if (subValues.size() == 4) {
                accessEntry.yyyy = subValues.get(0);
                accessEntry.mm = subValues.get(1);
                accessEntry.dd = subValues.get(2);
                accessEntry.ip = subValues.get(3);
            } else if (subValues.size() == 5) {
                accessEntry.yyyy = subValues.get(0);
                accessEntry.mm = subValues.get(1);
                accessEntry.dd = subValues.get(2);
                accessEntry.hh = subValues.get(3);
                accessEntry.ip = subValues.get(4);
            } else {
                accessEntry.ip = subValues.get(0);
            }

            // The time is split into 2 values so they have to be combined
            // then sent to match the time regular expression
            // "[02/Aug/2011:00:00:04" + " -0700]" = "[02/Aug/2011:00:00:04 -0700]"
            accessEntry.timestamp = new Timestamp(DATE_FORMAT.parse(csvRecord.get(3) + " " + csvRecord.get(4)).getTime());

            // The resource filed has 3 fields (HTTP Method, Page and HTTP protocol)
            // so it has to be further split by spaces
            String reqInfo = csvRecord.get(5);
            String[] reqInfoArr = reqInfo.split(" ");

            // Get the HTTP method
            accessEntry.method = reqInfoArr[0];

            // Get the page requested
            accessEntry.resource = reqInfoArr[1];

            // Get the HTTP response code
            accessEntry.httpCode = Integer.parseInt(csvRecord.get(6));

            // Try to get the response data size in bytes, if a hyphen shows up,
            // that means the client has a cache of this page and no data is
            // sent back
            try {
                accessEntry.dataLength = Long.parseLong(csvRecord.get(7));
            } catch (NumberFormatException nfe) {
                accessEntry.dataLength = 0;
            }

            if (csvRecord.size() >= 9) {
                accessEntry.referrer = csvRecord.get(8);
            }

            if (csvRecord.size() >= 10) {
                accessEntry.userAgent = csvRecord.get(9);
            }

            return accessEntry;
        } catch (IOException ioe) {
            //ioe.printStackTrace();
            return null;
        } catch (ParseException pe) {
            //pe.printStackTrace();
            return null;
        } catch (NumberFormatException nfe) {
            //nfe.printStackTrace();
            return null;
        }
    }

    public static void main(String[] arg) {
        String line = "46.235.68.229 - - [02/Jan/2012] \"GET /tags-tw-combined-17._V186434376_.js HTTP/1.1\" 200 \"http://www.incredibledeals.com/product/productDetails.jsp?PPSID=B002TLTGM6&parentPage=family&cookieHash=1970665683\" \"Mozilla/5.0 (X11; Linux x86_64; rv:5.0) Gecko/20100101 Firefox/5.0 FirePHP/0.5\" \"Monkey Bar Agent\"\n";
        //String line = "46.235.68.229 - - [02/Jan/2012:00:00:00 -0800] \"GET /tags-tw-combined-17._V186434376_.js HTTP/1.1\" 200 \"http://www.incredibledeals.com/product/productDetails.jsp?PPSID=B002TLTGM6&parentPage=family&cookieHash=1970665683\" \"Mozilla/5.0 (X11; Linux x86_64; rv:5.0) Gecko/20100101 Firefox/5.0 FirePHP/0.5\" \"Monkey Bar Agent\"\n";
        AccessLogParser parser = new AccessLogParser();
        ValueFeeder valueFeeder = parser.getValueFeeder(line);
        System.out.println("ip: " + valueFeeder.getValue("ip"));
        System.out.println("timestamp: " + valueFeeder.getValue("timestamp"));
        System.out.println("method: " + valueFeeder.getValue("method"));
        System.out.println("resource: " + valueFeeder.getValue("resource"));
        System.out.println("httpCode: " + valueFeeder.getValue("httpCode"));
        System.out.println("dataLength: " + valueFeeder.getValue("dataLength"));
        System.out.println("referrer: " + valueFeeder.getValue("referrer"));
        System.out.println("userAgent: " + valueFeeder.getValue("userAgent"));
        System.out.println("yyyy: " + valueFeeder.getValue("yyyy"));
        System.out.println("mm: " + valueFeeder.getValue("mm"));
        System.out.println("dd: " + valueFeeder.getValue("dd"));
        System.out.println("hh: " + valueFeeder.getValue("hh"));
    }
}
