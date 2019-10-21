package org.dcm.viewupdater;

import ddlogapi.DDlogAPI;
import ddlogapi.DDlogCommand;
import ddlogapi.DDlogRecord;
import org.dcm.IRTable;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class DDlogUpdater {
    private final DDlogAPI API;
    private final Map<String, IRTable> irTables;

    static final String INTEGER_TYPE = "java.lang.Integer";
    static final String STRING_TYPE = "java.lang.String";
    static final String BOOLEAN_TYPE = "java.lang.Boolean";
    static final String LONG_TYPE = "java.lang.Long";

    private ddlog.weave_fewer_queries_cap.weave_fewer_queries_capUpdateBuilder builder
            = new ddlog.weave_fewer_queries_cap.weave_fewer_queries_capUpdateBuilder();

    public DDlogUpdater(final Consumer<DDlogCommand> consumer, final Map<String, IRTable> irTables) {
        API = new DDlogAPI(1, consumer, false);
        API.record_commands("replay.dat", false);
        this.irTables = irTables;
    }

    public DDlogRecord toDDlogRecord(final String tableName, final List<Object> args) {
        final List<DDlogRecord> records = new ArrayList<>();
        final IRTable irTable = irTables.get(tableName);
        final Table<? extends Record> table = irTable.getTable();

        int counter = 0;
        for (final Field<?> field : table.fields()) {
            final Class<?> cls = field.getType();
            switch (cls.getName()) {
                case BOOLEAN_TYPE:
                    records.add(new DDlogRecord((Boolean) args.get(counter)));
                    break;
                case INTEGER_TYPE:
                    records.add(new DDlogRecord((Integer) args.get(counter)));
                    break;
                case LONG_TYPE:
                    records.add(new DDlogRecord((Long) args.get(counter)));
                    break;
                case STRING_TYPE:
                    records.add(new DDlogRecord(args.get(counter).toString().trim()));
                    break;
                default:
                    throw new RuntimeException("Unexpected datatype: " + cls.getName());
            }
            counter = counter + 1;
        }
        DDlogRecord[] recordsArray = new DDlogRecord[records.size()];
        recordsArray = records.toArray(recordsArray);
        return DDlogRecord.makeStruct(tableName, recordsArray);
    }

    public void sendUpdatesToDDlog(final Map<String, List<Object[]>> recordsFromDB2) {
        for (final Map.Entry<String, List<Object[]>> entry: recordsFromDB2.entrySet()) {
            final String tableName = entry.getKey();
            for (final Object[] row: entry.getValue()) {
                update(tableName, row);
            }
            recordsFromDB2.get(tableName).clear();
        }
        checkDDlogExitCode(API.start());
        checkDDlogExitCode(builder.applyUpdates(API));
        checkDDlogExitCode(API.commit());
        builder = new ddlog.weave_fewer_queries_cap.weave_fewer_queries_capUpdateBuilder();
    }

    private void checkDDlogExitCode(final int exitCode) {
        if (exitCode < 0) {
            throw new RuntimeException("Error executing " + exitCode);
        }
    }

    public void close() {
        API.stop();
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
