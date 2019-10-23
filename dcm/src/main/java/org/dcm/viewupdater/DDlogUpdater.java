package org.dcm.viewupdater;

import ddlogapi.DDlogAPI;
import ddlogapi.DDlogCommand;
import ddlogapi.DDlogException;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class DDlogUpdater {
    private final DDlogAPI API;

    private ddlog.weave_fewer_queries_cap.weave_fewer_queries_capUpdateBuilder builder
            = new ddlog.weave_fewer_queries_cap.weave_fewer_queries_capUpdateBuilder();

    Consumer<DDlogCommand<Object>> consumer;

    public DDlogUpdater(final Consumer<DDlogCommand<Object>> consumer) {
        this.consumer = consumer;
        try {
            API = new DDlogAPI(1, null, false);
            API.recordCommands("replay.dat", false);
        } catch (final DDlogException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void sendUpdatesToDDlog(final Map<String, List<Object[]>> recordsFromDB2) {
        try {
            API.transactionStart();
            for (final Map.Entry<String, List<Object[]>> entry: recordsFromDB2.entrySet()) {
                final String tableName = entry.getKey();
                for (final Object[] row: entry.getValue()) {
                    update(tableName, row);
                }
                recordsFromDB2.get(tableName).clear();
            }
            builder.applyUpdates(API);
            ddlog.weave_fewer_queries_cap.weave_fewer_queries_capUpdateParser
                    .transactionCommitDumpChanges(API, consumer);
        } catch (final DDlogException e) {
            throw new RuntimeException(e);
        }
        builder = new ddlog.weave_fewer_queries_cap.weave_fewer_queries_capUpdateBuilder();
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
                        new BigInteger(String.valueOf((long) newRow[4])),
                        new BigInteger(String.valueOf((long) newRow[5])),
                        new BigInteger(String.valueOf((long) newRow[6])),
                        new BigInteger(String.valueOf((long) newRow[7])),
                        newRow[8].toString(),
                        newRow[9].toString(),
                        new BigInteger(String.valueOf((int) newRow[10]))
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
                        new BigInteger(String.valueOf((long) newRow[9])),
                        new BigInteger(String.valueOf((long) newRow[10])),
                        new BigInteger(String.valueOf((long) newRow[11])),
                        new BigInteger(String.valueOf((long) newRow[12])),
                        new BigInteger(String.valueOf((long) newRow[13])),
                        new BigInteger(String.valueOf((long) newRow[14])),
                        new BigInteger(String.valueOf((long) newRow[15])),
                        new BigInteger(String.valueOf((long) newRow[16]))
                        );
                break;
            default:
                break;
        }
    }
}
