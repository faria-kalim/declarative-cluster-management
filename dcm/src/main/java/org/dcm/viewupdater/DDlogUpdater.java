package org.dcm.viewupdater;

import ddlogapi.DDlogAPI;
import ddlogapi.DDlogCommand;
import ddlogapi.DDlogException;
import ddlogapi.DDlogRecord;
import org.dcm.IRTable;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class DDlogUpdater {
    private final DDlogAPI API;

    private ddlog.weave_fewer_queries_cap.weave_fewer_queries_capUpdateBuilder builder
            = new ddlog.weave_fewer_queries_cap.weave_fewer_queries_capUpdateBuilder();

    public DDlogUpdater(final Consumer<DDlogCommand<DDlogRecord>> consumer, final Map<String, IRTable> irTables) {
        try {
            API = new DDlogAPI(1, consumer, false);
        } catch (final DDlogException e) {
            throw new RuntimeException(e);
        }
    }

    public void sendUpdatesToDDlog(final Map<String, List<Object[]>> recordsFromDB2) {
        for (final Map.Entry<String, List<Object[]>> entry: recordsFromDB2.entrySet()) {
            final String tableName = entry.getKey();
            for (final Object[] row: entry.getValue()) {
                update(tableName, row);
            }
            recordsFromDB2.get(tableName).clear();
        }
        try {
            API.transactionStart();
            builder.applyUpdates(API);
            API.transactionCommit();
            builder = new ddlog.weave_fewer_queries_cap.weave_fewer_queries_capUpdateBuilder();
        } catch (final DDlogException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            API.stop();
        } catch (final DDlogException e) {
            throw new RuntimeException(e);
        }
    }

    public void update(final String tableName, final Object[] newRow) {
        switch (tableName) {
            case "POD":
                builder.insert_POD(newRow[0].toString(),
                        newRow[1].toString(),
                        newRow[2].toString(),
                        newRow[3].toString(),
                        (long) newRow[4],
                        (long) newRow[5],
                        (long) newRow[6],
                        (long) newRow[7],
                        newRow[8].toString(),
                        newRow[9].toString(),
                        (int) newRow[10]
                );
                break;
            case "NODE":
                builder.insert_NODE(newRow[0].toString(),
                        (boolean) newRow[1],
                        (boolean) newRow[2],
                        (boolean) newRow[3],
                        (boolean) newRow[4],
                        (boolean) newRow[5],
                        (boolean) newRow[6],
                        (boolean) newRow[7],
                        (boolean) newRow[8],
                        (long) newRow[9],
                        (long) newRow[10],
                        (long) newRow[11],
                        (long) newRow[12],
                        (long) newRow[13],
                        (long) newRow[14],
                        (long) newRow[15],
                        (long) newRow[16]
                        );
                break;
            default:
                break;
        }
    }
}