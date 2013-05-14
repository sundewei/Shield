package com.sap.shield;

import org.restlet.Component;
import org.restlet.data.Protocol;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/23/12
 * Time: 2:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class RestServer {
    public static void main(String[] arg) throws Exception {
        System.out.println("About to load the metadata from DB to cache...");
        ShieldManager shieldManager = ShieldManager.getInstance();

        // Create a new Component.
        Component component = new Component();

        // Add a new HTTP server listening on port 8182.
        component.getServers().add(Protocol.HTTP, 8182);

        // Attach the sample application.
        component.getDefaultHost().attach("", new Application());

        // Start the component.
        component.start();
        System.out.println("Server started...");
    }
}
