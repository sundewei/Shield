package com.sap.shield;

import com.sap.shield.resources.*;
import com.sap.shield.resources.System;
import org.restlet.Restlet;
import org.restlet.routing.Router;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/22/12
 * Time: 3:48 PM
 * To change this template use File | Settings | File Templates.
 */
public class Application extends org.restlet.Application {
    /**
     * Creates a root Restlet that will receive all incoming event calls.
     */
    @Override
    public synchronized Restlet createInboundRoot() {
        // Create a router Restlet that routes each call to a new instance of HelloWorldResource.
        Router router = new Router(getContext());

        // Defines the event create route
        router.attach("/", Register.class);

        router.attach("/table/{tableId}", Upsert.class);

        router.attach("/query/{tableId}", Query.class);

        router.attach("/get/{tableId}", Get.class);

        router.attach("/result/{resultScannerKey}/{batch}", Loop.class);

        router.attach("/preparer/{resultScannerKey}/{tableName}", DbLoadPreparer.class);

        router.attach("/metadata/tableName/{tableId}", Metadata.class);

        router.attach("/metadata/tableId/{tableName}", Metadata.class);

        router.attach("/metadata/schema/{tableName}", Metadata.class);

        router.attach("/system", System.class);

        return router;
    }

    public static String getUpdateUrl(String hostname) {
        return "http://" + hostname + ":8080/shs/rest/";
    }
}
