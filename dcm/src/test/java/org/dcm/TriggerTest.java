package org.dcm;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.dcm.viewupdater.ViewUpdater;
import org.dcm.viewupdater.HSQLUpdater;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.jooq.impl.DSL.using;

public class TriggerTest {

    private DSLContext setupDerby() {
        final Properties properties = new Properties();
        properties.setProperty("foreign_keys", "true");
        try {
            // The following block ensures we always drop the database between tests
            try {
                final String dropUrl = "jdbc:derby:memory:test;drop=true";
                DriverManager.getConnection(dropUrl, properties);
            } catch (final SQLException e) {
                // We could not drop a database because it was never created. Move on.
            }
            // Create a fresh database
            final String connectionURL = "jdbc:derby:memory:db;create=true";
            final Connection conn = DriverManager.getConnection(connectionURL, properties);
            final DSLContext using = using(conn, SQLDialect.DERBY);
            using.execute("create schema curr");
            using.execute("set schema curr");
            return using;
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /*
     * Sets up an in-memory H2 database that we use for all tests.
     */
    private DSLContext setupH2() {
        final Properties properties = new Properties();
        properties.setProperty("foreign_keys", "true");
        try {
            // Create a fresh database
            final String connectionURL = "jdbc:h2" +
                    ":mem:;create=true";
            final Connection conn = DriverManager.getConnection(connectionURL, properties);
            final DSLContext using = using(conn, SQLDialect.H2);
            using.execute("create schema curr");
            using.execute("set schema curr");
            return using;
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }


    /*
     * Sets up an in-memory HSQLDB database that we use for all tests.
     */
    private DSLContext setupHSQLDB() {
        final Properties properties = new Properties();
        properties.setProperty("foreign_keys", "true");
        try {
            // Create a fresh database
            final String connectionURL = "jdbc:hsqldb:mem:db";
            final Connection conn = DriverManager.getConnection(connectionURL, properties);
            final DSLContext using = using(conn, SQLDialect.HSQLDB);
            using.execute("create schema curr");
            using.execute("set schema curr");
            return using;
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }


    /*
     * Sets up an in-memory Postgres database that we use for all tests.
     */
    private Connection setupPostgres() {
        try {
            final Connection connection = DriverManager.getConnection("jdbc:pgsql://127.0.0.1:5432/test");
            final Statement statement = connection.createStatement();
            statement.executeUpdate("drop schema public cascade;");
            statement.executeUpdate("create schema public;");
            statement.close();
            return connection;
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testLargerExample() {
//    final Connection conn = setupPostgres();
//    final DSLContext dbCtx = using(conn, SQLDialect.POSTGRES);

//    final DSLContext dbCtx = setupH2();
//    final DSLContext dbCtx = setupDerby();
//    final DSLContext dbCtx = setupHSQLDB();

        Connection conn;
        DSLContext dbCtx;
        final Properties properties = new Properties();
        properties.setProperty("foreign_keys", "true");
        try {
            // Create a fresh database
            final String connectionURL = "jdbc:hsqldb:mem:db";
            conn = DriverManager.getConnection(connectionURL, properties);
            dbCtx = using(conn, SQLDialect.HSQLDB);
            dbCtx.execute("create schema curr");
            dbCtx.execute("set schema curr");
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }

        dbCtx.execute("create table NODE\n" +
                "(\n" +
                " name varchar(36) not null primary key, "  +
                " is_master boolean not null, "  +
                " unschedulable boolean not null, "  +
                " out_of_disk boolean not null, "  +
                " memory_pressure boolean not null, "  +
                " disk_pressure boolean not null,"  +
                " pid_pressure boolean not null,"  +
                " ready boolean not null,"  +
                " network_unavailable boolean not null,"  +
                " cpu_capacity bigint not null,"  +
                " memory_capacity bigint not null,"  +
                " ephemeral_storage_capacity bigint not null,"  +
                " pods_capacity bigint not null,"  +
                " cpu_allocatable bigint not null,"  +
                " memory_allocatable bigint not null,"  +
                " ephemeral_storage_allocatable bigint not null,"  +
                " pods_allocatable bigint not null)"
        );
        dbCtx.execute("create table POD\n" +
                "(\n" +
                " pod_name varchar(100) not null primary key,\n" +
                " status varchar(36) not null,\n" +
                " node_name varchar(36) not null,\n" +
                " namespace varchar(100) not null,\n" +
                " cpu_request bigint not null,\n" +
                " memory_request bigint not null,\n" +
                " ephemeral_storage_request bigint not null,\n" +
                " pods_request bigint not null,\n" +
                " owner_name varchar(100) not null,\n" +
                " creation_timestamp varchar(100) not null,\n" +
                " priority integer not null)"
        );

        dbCtx.execute("create table SPARECAPACITY\n" +
                "(\n" +
                "  name varchar(36) not null,\n" +
                "  cpu_remaining bigint not null,  " +
                "  memory_remaining bigint not null, " +
                "  pods_remaining bigint not null " +  ")"
        );

        final List<String> baseTables = new ArrayList<>();
        baseTables.add("POD");
        baseTables.add("NODE");

        final Model model =
                buildModel(dbCtx, new ArrayList<>(), "testModel");

        ViewUpdater.setIRTables(model.getIRTables());
        final ViewUpdater updater = new HSQLUpdater(conn, dbCtx, baseTables);
//        final ViewUpdater updater = new H2Updater(dbCtx, baseTables);
//        final ViewUpdater updater = new DerbyUpdater(dbCtx, baseTables);
//        final ViewUpdater updater = new PGUpdater(conn, dbCtx, baseTables);

//             final Updater updater = new Updater(conn, dbCtx, baseTables, model.getIRTables());
//            updater.setUpdater(new DDlogUpdater(r -> updater.receiveUpdateFromDDlog(r)));

        try {
        final PreparedStatement nodeStmt = conn.prepareStatement(
                "insert into node values(?, false, false, false, false, " +
                        "false, false, false, false, 1, 1, 1, 1, 1, 1, 1, 1)");
        final PreparedStatement podStmt = conn.prepareStatement(
                "insert into pod values(?, 'scheduled', ?, 'default', 1, 1, 1, 1, 'owner', 'owner', 1)");

        for (int j = 0; j < 50; j++) {
            final int numRecords = 100000;
            int index = j * numRecords;
            final int iEnd = index + numRecords;
            for (; index < iEnd; index++) {

                final String node = "node" + index;
                final String pod = "pod" + index;
                nodeStmt.setString(1, node);

                podStmt.setString(1, pod);
                podStmt.setString(2, node);

                nodeStmt.executeUpdate();
                podStmt.executeUpdate();
            }
            updater.flushUpdates();
        }

        } catch (final SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Builds a Model for a test case, making sure to:
     *  - Build initial model (without hand-written constraints) on the first run. Note: the test FAILS on this scenario
     *  - Build the model with an already existing file. You should edit that file in between runs.
     *
     * @param dbCtx Connection to the DB
     * @param testName Name of the test case. Model and data files will be based on that name
     * @return built Model
     */
    @CanIgnoreReturnValue
    private Model buildModel(final DSLContext dbCtx, final List<String> views, final String testName) {
        final File modelFile = new File("src/test/resources/" + testName + ".mzn");
        // create data file
        final File dataFile = new File("/tmp/" + testName + ".dzn");

        return Model.buildModel(dbCtx, views, modelFile, dataFile);
    }
}