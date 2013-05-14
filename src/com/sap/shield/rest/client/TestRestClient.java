package com.sap.shield.rest.client;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.*;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/25/12
 * Time: 4:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestRestClient extends Thread {
    DefaultHttpClient client = new DefaultHttpClient();

    public String createTable(String restUrl) throws IOException {
        HttpPost httpPost = new HttpPost(restUrl);
        httpPost.addHeader("Content-Type", "application/xml");
        StringEntity stringEntity = new StringEntity(FileUtils.readFileToString(new File("C:\\projects\\Shield\\data\\createTableReq.xml")));
        httpPost.setEntity(stringEntity);
        HttpResponse response = client.execute(httpPost);
        HttpEntity entity = response.getEntity();
        StringWriter writer = new StringWriter();
        IOUtils.copy(entity.getContent(), writer);
        return writer.toString();
    }

    public String updateTable(String restUrl) throws IOException {
        HttpPut httpPut = new HttpPut(restUrl);
        httpPut.addHeader("Content-Type", "application/xml");
        StringEntity stringEntity = new StringEntity(FileUtils.readFileToString(new File("C:\\projects\\Shield\\data\\updateTableReq.xml")));
        httpPut.setEntity(stringEntity);
        HttpResponse response = client.execute(httpPut);
        HttpEntity entity = response.getEntity();
        StringWriter writer = new StringWriter();
        IOUtils.copy(entity.getContent(), writer);
        return writer.toString();
    }

    public String sendEvent(String restUrl) throws IOException {
        HttpPost httpPost = new HttpPost(restUrl);
        httpPost.addHeader("Content-Type", "text/csv");
        StringEntity stringEntity = new StringEntity(FileUtils.readFileToString(new File("C:\\data\\2012-01-02.log")));
        httpPost.setEntity(stringEntity);
        HttpResponse response = client.execute(httpPost);
        HttpEntity entity = response.getEntity();
        StringWriter writer = new StringWriter();
        IOUtils.copy(entity.getContent(), writer);
        return writer.toString();
    }

    public String deleteTable(String restUrl) throws IOException {
        HttpDelete httpDelete = new HttpDelete(restUrl);
        HttpResponse response = client.execute(httpDelete);
        HttpEntity entity = response.getEntity();
        if (response.getStatusLine().getStatusCode() != 200) {
            System.out.println(response.getStatusLine().getReasonPhrase());
        }
        StringWriter writer = new StringWriter();
        IOUtils.copy(entity.getContent(), writer);
        return writer.toString();
    }

    public String sendEvent(String restUrl, String line) throws IOException {
        HttpPost httpPost = new HttpPost(restUrl);
        httpPost.addHeader("Content-Type", "text/csv");
        StringEntity stringEntity = new StringEntity(line);
        httpPost.setEntity(stringEntity);
        HttpResponse response = client.execute(httpPost);
        HttpEntity entity = response.getEntity();
        StringWriter writer = new StringWriter();
        IOUtils.copy(entity.getContent(), writer);
        return writer.toString();
    }

    public String query(String restUrl) throws IOException {
        HttpPost httpPost = new HttpPost(restUrl);
        httpPost.addHeader("Content-Type", "application/xml");
        StringEntity stringEntity = new StringEntity(FileUtils.readFileToString(new File("C:\\projects\\Shield\\data\\myQuery.xml")));
        httpPost.setEntity(stringEntity);
        HttpResponse response = client.execute(httpPost);
        if (response.getStatusLine().getStatusCode() != 200) {
            System.out.println(response.getStatusLine().getReasonPhrase());
        }
        HttpEntity entity = response.getEntity();
        StringWriter writer = new StringWriter();
        IOUtils.copy(entity.getContent(), writer);
        return writer.toString();
    }

    public String getFetch(String restUrl) throws IOException {
        HttpPost httpPost = new HttpPost(restUrl);
        httpPost.addHeader("Content-Type", "application/xml");
        StringEntity stringEntity = new StringEntity(FileUtils.readFileToString(new File("C:\\projects\\Shield\\data\\getQueryReq.xml")));
        httpPost.setEntity(stringEntity);
        HttpResponse response = client.execute(httpPost);
        if (response.getStatusLine().getStatusCode() != 200) {
            System.out.println(response.getStatusLine().getReasonPhrase());
        }
        HttpEntity entity = response.getEntity();
        StringWriter writer = new StringWriter();
        IOUtils.copy(entity.getContent(), writer);
        return writer.toString();
    }

    public String sendPost(String restUrl) throws IOException {
        HttpPost httpPost = new HttpPost(restUrl);
        httpPost.addHeader("Content-Type", "text/csv");
        //StringEntity stringEntity = new StringEntity(FileUtils.readFileToString(new File("C:\\data\\183k.csv")));
        StringEntity stringEntity = new StringEntity(FileUtils.readFileToString(new File("C:\\data\\3.csv")));
        //StringEntity stringEntity = new StringEntity(FileUtils.readFileToString(new File("C:\\data\\1.csv")));
        httpPost.setEntity(stringEntity);
        HttpResponse response = client.execute(httpPost);
        HttpEntity entity = response.getEntity();
        StringWriter writer = new StringWriter();
        IOUtils.copy(entity.getContent(), writer);
        return writer.toString();
    }

    public String loop(String restUrl) throws IOException {
        HttpGet httpGet = new HttpGet(restUrl);
        HttpResponse response = client.execute(httpGet);
        HttpEntity entity = response.getEntity();
        StringWriter writer = new StringWriter();
        IOUtils.copy(entity.getContent(), writer);
        return writer.toString();
    }

    @Override
    public void run() {
        /*
        int count = 0;
        while (messageCount > count) {
            String str1 = "ssssss_" + count;
            System.out.println(str1);
            int int2 = (int)(Math.random() * Integer.MAX_VALUE);
            long lo3 = (long)(Math.random() * Long.MAX_VALUE);
            double db4 = Math.random() * Double.MAX_VALUE;
            String line = CSVUtils.printLine(new String[]{str1, String.valueOf(int2), String.valueOf(lo3), String.valueOf(db4)});
            try {
                sendEvent(Constants.SELF_TEST_REST_URL + "/


                shield/events/22", line);
                Thread.sleep((int)(1000 * Math.random()));
            } catch (Exception ioe) {
                ioe.printStackTrace();
            }
            count++;
        }
        */
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File("C:\\data\\2008-01-01\\2008-01-01.log")));
            String line = reader.readLine();
            while (line != null) {
                System.out.println(line);
                sendEvent(Constants.SELF_TEST_REST_URL + "/table/22", line);
                line = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] arg) throws Exception {
        //TestRestClient client = new TestRestClient();
        //System.out.println(client.sendPost("http://lspal134:8010/shield/xs/project/shield.xsjs"));
        //System.out.println(client.deleteTable(Constants.SELF_TEST_REST_URL + "/table/22"));
        //System.out.println(client.createTable(Constants.SELF_TEST_REST_URL + "/"));
        //System.out.println(client.updateTable(Constants.SELF_TEST_REST_URL + "/table/22"));
        // System.out.println(client.sendEvent(Constants.SELF_TEST_REST_URL + "/table/22"));
        //String rsKey = client.query(Constants.SELF_TEST_REST_URL + "/query/22");
        //System.out.println("-------------------->" + rsKey);
        /*
        String csv = client.loop(Constants.TOMCAT_SELF_TEST_REST_URL + "/result/" + rsKey + "/1000");
        while (csv.indexOf(rsKey) < 0) {
            System.out.println("--->" + csv + "<---");
            csv = client.loop(Constants.TOMCAT_SELF_TEST_REST_URL + "/result/" + rsKey + "/1000");
        }
        */


        //System.out.println(client.getFetch(Constants.MASTER_TEST_REST_URL + "/get/22"));
        //System.out.println(client.createTable(Constants.SELF_TEST_REST_URL + "/"));
        //client.start();



        //String url = "http://llbpal36:8888/demoSensorData/1362178930250~wm_16/cf:windSpeed/";
        String url = "http://llbpal36:8888/demoSensorData/schema";
        //HttpGet httpGet = new HttpGet(url);
        HttpDelete httpDelete = new HttpDelete(url);
        DefaultHttpClient client = new DefaultHttpClient();
        HttpResponse response = client.execute(httpDelete);
        System.out.println(response.getStatusLine().getStatusCode());
        System.out.println(response.getStatusLine().getReasonPhrase());
        //byte[] bytes = IOUtils.toByteArray(response.getEntity().getContent());
        //System.out.println(Bytes.toInt(bytes));
        //System.out.println(new String(response.getEntity().getContent()));
    }
}
