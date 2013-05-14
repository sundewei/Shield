package com.sap.shield.rest.client;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/24/12
 * Time: 3:07 PM
 * To change this template use File | Settings | File Templates.
 */
public class HbaseRestClient {
    private DefaultHttpClient client = new DefaultHttpClient();

    public String sentPost(String restUrl, AbstractHttpEntity reqEntity) throws IOException {
        HttpPut httpPut = new HttpPut(restUrl);
        httpPut.addHeader("Content-Type", "text/xml");
        //httpPost.addHeader("Content-Type", "application/x-protobuf");
        httpPut.setEntity(reqEntity);
        HttpResponse response = client.execute(httpPut);
        //System.out.println("response.getStatusLine().getStatusCode()=" + response.getStatusLine().getStatusCode());
        //System.out.println("response.getStatusLine().getReasonPhrase()=" + response.getStatusLine().getReasonPhrase());

        HttpEntity entity = response.getEntity();
        StringWriter writer = new StringWriter();
        IOUtils.copy(entity.getContent(), writer);
        return writer.toString();
    }

    public static void main(String[] arg) throws Exception {
        HbaseRestClient client = new HbaseRestClient();
        StringEntity stringEntity = new StringEntity(FileUtils.readFileToString(new File("C:\\projects\\Shield\\data\\hbaseInsert.xml")));
        //System.out.println(client.sentPost(Constants.HBASE_REST_BASE_URL + "/tableName2/123", stringEntity));
    }
}
