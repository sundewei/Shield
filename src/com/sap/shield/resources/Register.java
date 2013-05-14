package com.sap.shield.resources;

import com.sap.shield.ActionHelper;
import com.sap.shield.Constants;
import com.sap.shield.HbaseHelper;
import com.sap.shield.ShieldManager;
import com.sap.shield.exceptions.HbaseShieldTableExistException;
import com.sap.shield.exceptions.ShieldException;
import com.sap.shield.messages.CreateTableRequest;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.restlet.resource.Post;
import org.restlet.resource.ServerResource;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/23/12
 * Time: 10:26 AM
 * To change this template use File | Settings | File Templates.
 */
public class Register extends ServerResource {

    private static final Logger LOG = Logger.getLogger(Register.class.getName());

    /**
     * Reload the server's cache
     *
     * @param entity response representation
     * @return "ok" if reload successfully
     */
    @Get()
    public Representation get(Representation entity) {
        Constants.MASTER_URL = getRequest().getClientInfo().getAddress();
        Constants.MASTER_PORT = getRequest().getHostRef().getHostPort();
        Constants.MASTER_SCHEME = getRequest().getHostRef().getScheme();
        ShieldManager.getInstance().updateAll();
        Constants.MASTER_URL = null;
        Constants.MASTER_SCHEME = null;
        ShieldManager.getInstance().setRefreshing(false);
        return new StringRepresentation("ok");
    }

    /**
     * Insert the table schema into HANA and create a hbase table
     *
     * @param entity the request representation
     * @return the response representation
     */
    @Post()
    public Representation post(Representation entity) {
        String movedUrl = ShieldManager.getInstance().getMovedUrl(getRequest().getHostRef().getPath());
        if (movedUrl != null) {
            this.setStatus(Status.REDIRECTION_TEMPORARY);
            return new StringRepresentation(movedUrl);
        }

        CreateTableRequest createTableRequest = null;
        Representation representation = null;
        try {
            if (!entity.getMediaType().toString().matches(MediaType.APPLICATION_ALL_XML.toString())) {
                throw new ShieldException("MediaType not supported: " + entity.getMediaType().toString());
            }
            createTableRequest = ActionHelper.getInstance().getCreateTableRequestFromXml(entity.getText());
            createTable(createTableRequest);
            ShieldManager.getInstance().addUpdateSequence();
            representation = new StringRepresentation(String.valueOf(createTableRequest.getPropertyInt(Constants.TABLE_ID)));
        } catch (IOException ioe) {
            this.setStatus(Status.SERVER_ERROR_INTERNAL, ioe.getMessage());
            LOG.log(Level.WARNING, ioe.getMessage(), ioe);
        } catch (SQLException sqle) {
            this.setStatus(Status.SERVER_ERROR_INTERNAL, sqle.getMessage());
            LOG.log(Level.WARNING, sqle.getMessage(), sqle);
        } catch (ShieldException se) {
            this.setStatus(Status.CLIENT_ERROR_BAD_REQUEST, se.getMessage());
            LOG.log(Level.INFO, se.getMessage(), se);
        }
        return representation;
    }

    private void createTable(CreateTableRequest request) throws IOException, SQLException, ShieldException {
        if (HbaseHelper.getInstance().tableExists(request.getPropertyString(Constants.TABLE_NAME))) {
            throw new HbaseShieldTableExistException("Table '" + request.getPropertyString(Constants.TABLE_NAME) + "' already exists.");
        }
        HbaseHelper.getInstance().createTable(request.getPropertyString(Constants.TABLE_NAME), Arrays.asList(Constants.DATA_COLUMN_FAMILY, Constants.METADATA_COLUMN_FAMILY));
        ShieldManager.getInstance().createShieldTable(request);
    }
}
