package com.sap.shield.resources;

import com.sap.shield.cache.ShieldColumns;
import com.sap.shield.cache.ShieldTableColumns;
import com.sap.shield.cache.ShieldTables;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

import java.util.Map;
import java.util.TreeSet;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 11/14/12
 * Time: 2:38 PM
 * To change this template use File | Settings | File Templates.
 */
public class System extends ServerResource {
    @Get()
    public Representation get(Representation entity) {
        StringBuilder sb = new StringBuilder();
        sb.append("ShieldTables: \n");
        sb.append("    Current Max Id: ").append(ShieldTables.getMaxId()).append("\n");
        for (Map.Entry<Integer, ShieldTables.Row> entry : ShieldTables.DATA.entrySet()) {
            sb.append(entry.getValue()).append("\n");
        }
        sb.append("=================================================================").append("\n");

        sb.append("ShieldColumns: \n");
        sb.append("    Current Max Id: ").append(ShieldColumns.getMaxId()).append("\n");
        for (Map.Entry<Integer, ShieldColumns.Row> entry : ShieldColumns.DATA.entrySet()) {
            sb.append(entry.getValue()).append("\n");
        }
        sb.append("=================================================================").append("\n");

        sb.append("ShieldTableColumns: \n");
        for (Map.Entry<Integer, TreeSet<ShieldTableColumns.Row>> entry : ShieldTableColumns.DATA_BY_TABLE_ID.entrySet()) {
            TreeSet<ShieldTableColumns.Row> rows = entry.getValue();
            sb.append("    Table Id: ").append(entry.getKey().intValue()).append("\n");
            for (ShieldTableColumns.Row row : rows) {
                sb.append(row).append("\n");
            }
            sb.append("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -").append("\n");
        }
        return new StringRepresentation(sb.toString());
    }
}
