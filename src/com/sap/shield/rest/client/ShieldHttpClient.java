package com.sap.shield.rest.client;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 11/14/12
 * Time: 12:22 PM
 * To change this template use File | Settings | File Templates.
 */
public class ShieldHttpClient {
    DefaultHttpClient client = new DefaultHttpClient();

    public void sendGet(String restUrl) throws IOException {
        HttpGet httpGet = new HttpGet(restUrl);
        httpGet.addHeader("Content-Type", "application/xml");
        HttpResponse response = client.execute(httpGet);
        HttpEntity entity = response.getEntity();
        StringWriter stringWriter = new StringWriter();
        InputStream in = entity.getContent();
        IOUtils.copy(in, stringWriter);
        IOUtils.closeQuietly(in);
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new IOException(response.getStatusLine().getReasonPhrase());
        }
    }
}
