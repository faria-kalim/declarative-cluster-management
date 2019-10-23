package org.dcm.viewupdater;

import ddlog.weave_fewer_queries_cap.SPARECAPACITYReader;
import ddlog.weave_fewer_queries_cap.weave_fewer_queries_capRelation;
import ddlogapi.DDlogCommand;
import org.dcm.IRTable;
import org.jooq.DSLContext;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * ViewUpdater can be used to help update views held in the DB incrementally, using DDLog. The class receives triggers
 * based on updates on base tables in the DB. These updates are held in memory until the user calls the function
 * "flushUpdates". Then, these updates are passed to DDlog, that incrementally computes views on them and returns
 * updates. Finally, we push these updates back to the DB.
 */
public abstract class ViewUpdater {
    String triggerClassName;
    String modelName;

    final Connection connection;
    private final List<String> baseTables;
    private final DSLContext dbCtx;
    private final Map<String, Map<String, PreparedStatement>> preparedQueries = new HashMap<>();
    private Map<String, IRTable> irTables;
    private final DDlogUpdater updater;
    private final List<List<Object>> recordsFromDDLog = new ArrayList<>();
    private static final String SPARECAPACITY_NAME = "SPARECAPACITY";
    static final Map<String, List<Object[]>> RECORDS_FROM_DB_2 = new HashMap<>();

    /**
     * @param modelName: the name of the model this object is associated with
     *                 This allows us to only utilize records from the DB that are related to our current model.
     * @param connection: a connection to the DB used to build prepared statements
     * @param dbCtx: database context, mainly used to create triggers.
     * @param baseTables: the tables we build triggers for
     * @param irTables: the datastructure that gives us schema for the "base" and "view" tables
     */
    public ViewUpdater(final String modelName, final Connection connection, final DSLContext dbCtx,
                       final List<String> baseTables, final Map<String, IRTable> irTables) {
        this.modelName = modelName.toUpperCase(Locale.US);
        this.connection = connection;
        this.irTables = irTables;
        this.baseTables = baseTables;
        this.dbCtx = dbCtx;
        this.updater = new DDlogUpdater(r -> receiveUpdateFromDDlog(r));
    }

    void createDBTriggers() {
        for (final String entry : baseTables) {
            final String tableName = entry.toUpperCase(Locale.US);
            if (irTables.containsKey(tableName)) {
                final String triggerName = modelName + "_TRIGGER_" + tableName;

                final StringBuilder builder = new StringBuilder();
                builder.append("CREATE TRIGGER " + triggerName + " " + "BEFORE INSERT ON " + tableName + " " +
                        "FOR EACH ROW CALL \"" + triggerClassName + "\"");

                final String command = builder.toString();
                dbCtx.execute(command);
            }
        }
    }

    private void receiveUpdateFromDDlog(final DDlogCommand<Object> command) {
        final int relid = command.relid();
        switch (relid) {
            case weave_fewer_queries_capRelation.SPARECAPACITY: {
                final SPARECAPACITYReader reader = (SPARECAPACITYReader) command.value();
                final List<Object> temp = new ArrayList<>();
                temp.add(reader.name());
                temp.add(reader.cpu_remaining());
                temp.add(reader.memory_remaining());
                temp.add(reader.pods_remaining());
                recordsFromDDLog.add(temp);
                break;
            }
            default:
                break;
        }
    }

    public void flushUpdates() {
        updater.sendUpdatesToDDlog(RECORDS_FROM_DB_2);
        for (final List<Object> command : recordsFromDDLog) {
            // check if query is already created and if not, create it
            if (!preparedQueries.containsKey(SPARECAPACITY_NAME) ||
                    (preparedQueries.containsKey(SPARECAPACITY_NAME) &&
                            !preparedQueries.get(SPARECAPACITY_NAME).containsKey("MERGE"))) {
                preparedQueries.computeIfAbsent(SPARECAPACITY_NAME, k -> new HashMap<>());
                try {
                    preparedQueries.get(SPARECAPACITY_NAME).put("MERGE",
                            connection.prepareStatement(
                            "MERGE INTO SPARECAPACITY KEY(name) VALUES (?, ?, ?, ?)"));
                } catch (final SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            flush(command);
        }
        recordsFromDDLog.clear();
        RECORDS_FROM_DB_2.clear();
    }

     private void flush(final List<Object> command) {
        try {
            final PreparedStatement query = preparedQueries.get(SPARECAPACITY_NAME).get("MERGE");
            query.setString(1, (String) command.get(0));
            query.setInt(2,  ((BigInteger) command.get(1)).intValue());
            query.setInt(3,  ((BigInteger) command.get(2)).intValue());
            query.setInt(4,  ((BigInteger) command.get(3)).intValue());
            query.executeUpdate();
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            updater.close();
            connection.close();
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }
}