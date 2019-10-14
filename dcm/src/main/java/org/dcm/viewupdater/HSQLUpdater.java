package org.dcm.viewupdater;

import ddlogapi.DDlogCommand;
import ddlogapi.DDlogRecord;
import org.dcm.IRTable;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class HSQLUpdater extends ViewUpdater {
    private static DDlogUpdater updater = new DDlogUpdater(r -> receiveUpdateFromDDlog(r));
    private final Connection connection;
    private static final Map<String, List<LocalDDlogCommand>> RECEIVED_UPDATES = new HashMap<>();
    private final Map<String, Map<String, PreparedStatement>> preparedQueries = new HashMap<>();

    public HSQLUpdater(final Connection connection, final DSLContext dbCtx, final List<String> baseTables) {
        super(dbCtx, baseTables);
        this.connection = connection;
        createDBTriggers();
    }

    public static void receiveUpdateFromDDlog(final DDlogCommand command) {
        final DDlogRecord record = command.value;
        final String dataType = record.getStructName();
        if (irTables.containsKey(dataType)) {
            if (!RECEIVED_UPDATES.containsKey(dataType)) {
                RECEIVED_UPDATES.put(dataType, new ArrayList<>());
            }

            final LocalDDlogCommand cmd = new LocalDDlogCommand(String.valueOf(command.kind));

            final IRTable table = irTables.get(dataType);
            final Table<? extends Record> t = table.getTable();
            final Field[] fields = t.fields();

            if (command.kind == DDlogCommand.Kind.Insert) {
                for (int i = 0; i < fields.length; i++) {
                    final Class fieldClass = fields[i].getType();
                    final DDlogRecord item = record.getStructField(i);
                    switch (fieldClass.getName()) {
                        case LONG_TYPE:
                            cmd.values.add(item.getLong());
                            break;
                        case INTEGER_TYPE:
                            cmd.values.add(item.getU128());
                            break;
                        case BOOLEAN_TYPE:
                            cmd.values.add(item.getBoolean());
                            break;
                        default:
                            cmd.values.add(item.getString());
                    }
                }
            }
            RECEIVED_UPDATES.get(dataType).add(cmd);
        }
    }

    public void createDBTriggers() {
        for (final String entry : baseTables) {
            final String tableName = entry.toUpperCase(Locale.US);
            if (irTables.containsKey(tableName)) {
                final String triggerName = "TRIGGER_" + tableName;

                final StringBuilder builder = new StringBuilder();
                builder.append("CREATE TRIGGER " + triggerName + " " + "BEFORE INSERT ON " + tableName + " " +
                        "FOR EACH ROW CALL \"" + HSQLUpdater.InnerHSQLUpdater.class.getName() + "\"");

                final String command = builder.toString();
                dbCtx.execute(command);
            }
        }
    }

    static class LocalDDlogCommand {
        private String command;
        List values;

        LocalDDlogCommand(final String command) {
            this.command = command;
            this.values = new ArrayList();
        }
    }

    @Override
    public void flushUpdates() {
        updater.sendUpdatesToDDlog();

        for (final Map.Entry<String, List<LocalDDlogCommand>> entry: RECEIVED_UPDATES.entrySet()) {
            final String tableName = entry.getKey();
            final List<LocalDDlogCommand> commands = entry.getValue();
            for (final LocalDDlogCommand command : commands) {
                // check if query is already created and if not, create it
                if (!preparedQueries.containsKey(tableName) ||
                        (preparedQueries.containsKey(tableName) &&
                                !preparedQueries.get(tableName).containsKey(command.command))) {
                    updatePreparedQueries(tableName, command);
                }
                flush(tableName, command);
            }
        }
        RECEIVED_UPDATES.clear();
    }

    private void flush(final String tableName, final LocalDDlogCommand command) {
        try {
            final PreparedStatement query = preparedQueries.get(tableName).get(command.command);

            final IRTable irTable = irTables.get(tableName);
            final Table<? extends Record> table = irTable.getTable();
            final Field[] fields = table.fields();
            for (int i = 0; i < fields.length; i++) {
                final Class fieldClass = fields[i].getType();
                final Object item = command.values.get(i);
                final int index = i + 1;
                switch (fieldClass.getName()) {
                    case LONG_TYPE:
                        query.setLong(index, (Long) item);
                        break;
                    case INTEGER_TYPE:
                        query.setInt(index, (Integer) item);
                        break;
                    case BOOLEAN_TYPE:
                        query.setBoolean(index, (Boolean) item);
                        break;
                    default:
                        query.setString(index, (String) item);
                }
            }

            query.executeUpdate();
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }


    private void updatePreparedQueries(final String tableName, final LocalDDlogCommand command) {
        final String commandKind = command.command;
        if (!preparedQueries.containsKey(tableName)) {
            preparedQueries.put(tableName, new HashMap<>());
        }
        if (!preparedQueries.get(tableName).containsKey(commandKind)) {
            // make prepared statement here
            @SuppressWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
            final String preparedQuery = generatePreparedQueryString(tableName, String.valueOf(command.command));
            try {
                preparedQueries.get(tableName).put(commandKind, connection.prepareStatement(preparedQuery));
            } catch (final SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private String generatePreparedQueryString(final String dataType, final String commandKind) {
        final StringBuilder stringBuilder = new StringBuilder();
        final IRTable irTable = irTables.get(dataType);

        final Table<? extends Record> table = irTable.getTable();
        final Field[] fields = table.fields();
        if (commandKind.equals(String.valueOf(DDlogCommand.Kind.Insert))) {
            stringBuilder.append("insert into ").append(dataType).append(" values ( \n");
            stringBuilder.append(" ?,".repeat(Math.max(0, fields.length - 1)));
            stringBuilder.append(" ?");
        } else if (commandKind.equals(String.valueOf(DDlogCommand.Kind.DeleteVal))) {
            stringBuilder.append("delete from ").append(dataType).append(" where ( \n");
            int counter = 0;
            for (final Field field : fields) {
                stringBuilder.append(field.getName()).append(" = ?");
                if (counter < fields.length - 1) {
                    stringBuilder.append(" and ");
                }
                counter = counter + 1;
            }
        }
        stringBuilder.append("\n)");
        return stringBuilder.toString();
    }

    public static class InnerHSQLUpdater implements org.hsqldb.Trigger {
        final Map<String, IRTable> IR_TABLES = HSQLUpdater.irTables;

        @Override
        public void fire(final int type, final String trigName,
                         final String tabName, final Object[] oldRow, final Object[] newRow) {
            final DDlogRecord ddlogRecord = toDDlogRecord(tabName, newRow);
            updater.updateAndHold(ddlogRecord);
        }

        DDlogRecord toDDlogRecord(final String tableName, final Object[] args) {
            final List<DDlogRecord> records = new ArrayList<>();
            final IRTable irTable = IR_TABLES.get(tableName);
            final Table<? extends Record> table = irTable.getTable();

            int counter = 0;
            for (final Field<?> field : table.fields()) {
                final Class<?> cls = field.getType();
                switch (cls.getName()) {
                    case BOOLEAN_TYPE:
                        records.add(new DDlogRecord((Boolean) args[counter]));
                        break;
                    case INTEGER_TYPE:
                        records.add(new DDlogRecord((Integer) args[counter]));
                        break;
                    case LONG_TYPE:
                        records.add(new DDlogRecord((Long) args[counter]));
                        break;
                    case STRING_TYPE:
                        records.add(new DDlogRecord(args[counter].toString().trim()));
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
    }
}
