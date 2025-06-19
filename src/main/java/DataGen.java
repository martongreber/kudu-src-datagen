import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.apache.kudu.shaded.com.google.common.collect.ImmutableList;

/**
 * Invoke with cli argument 'clean' to delete the table if exists, and start inserting with int key starting from 0.
 * If clean is not provided, we are expecting the table to be in place, and we resume inserting data.
 */
public class DataGen {
    private static final String KUDU_MASTERS = System.getProperty("kuduMasters", "localhost:8764");
    private static final Random RANDOM = new Random();

    public static void main(String[] args) {
        System.out.println("-----------------------------------------------");
        System.out.println("Connecting to Kudu master(s) at " + KUDU_MASTERS);
        System.out.println("Run with -DkuduMasters=master-0:port,master-1:port,... to override.");
        System.out.println("-----------------------------------------------");

        String tableName = "test_table";
        try (KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build()) {
            boolean cleanStart = args.length > 0 && args[0].equalsIgnoreCase("clean");

            if (cleanStart) {
                cleanStartInsert(client, tableName);
            } else {
                resumeInsertFromMaxKey(client, tableName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /** Clean start: Deletes the table (if exists), recreates it, and starts inserting from 1 */
    static void cleanStartInsert(KuduClient client, String tableName) throws KuduException, InterruptedException {
        if (client.tableExists(tableName)) {
            System.out.println("Table " + tableName + " exists. Deleting...");
            client.deleteTable(tableName);
            System.out.println("Deleted table " + tableName);
        }
        if(client.tableExists("test_table_backup")) {
            System.out.println("Table " + "test_table_backup" + " exists. Deleting...");
            client.deleteTable("test_table_backup");
            System.out.println("Deleted table " + "test_table_backup");
        }

        createExampleTable(client, tableName);
        createExampleTable(client, "test_table_backup");
        insertRows(client, tableName, 1);
    }

    /** Resume insert: Finds the max key from existing table and continues inserting */
    static void resumeInsertFromMaxKey(KuduClient client, String tableName) throws KuduException, InterruptedException {
        if (!client.tableExists(tableName)) {
            System.out.println("Table does not exist, creating a new one.");
            createExampleTable(client, tableName);
            insertRows(client, tableName, 1);
        } else {
            int maxKey = getMaxKey(client, tableName);
            System.out.println("Resuming insert from key: " + (maxKey + 1));
            insertRows(client, tableName, maxKey + 1);
        }
    }

    /** Create table if it doesn’t exist */
    static void createExampleTable(KuduClient client, String tableName) throws KuduException {
        List<ColumnSchema> columns = new ArrayList<>();
        columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).nonUniqueKey(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING).nullable(true).build());
        Schema schema = new Schema(columns);

        CreateTableOptions options = new CreateTableOptions();
        options.addHashPartitions(ImmutableList.of("key"), 2);

        client.createTable(tableName, schema, options);
        System.out.println("Created table: " + tableName);
    }

    /** Insert rows starting from given key */
    static void insertRows(KuduClient client, String tableName, int startKey) throws KuduException, InterruptedException {
        KuduTable table = client.openTable(tableName);
            KuduSession session = client.newSession();
            session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
            int key = startKey;

            while (true) {
                Insert insert = table.newInsert();
                PartialRow row = insert.getRow();
                row.addInt("key", key);
                row.addString("value", generateRandomString(5));
                session.apply(insert);

                if (session.countPendingErrors() != 0) {
                    System.out.println("Errors inserting row with key " + key);
                    for (RowError err : session.getPendingErrors().getRowErrors()) {
                        System.out.println(err);
                    }
                } else {
                    System.out.println("Inserted row with key " + key);
                }

                key++;
                Thread.sleep(1000);
            }
    }

    /** Get the max key from the table using a full scan */
    static int getMaxKey(KuduClient client, String tableName) throws KuduException {
        KuduTable table = client.openTable(tableName);
        KuduScanner scanner = client.newScannerBuilder(table)
                .setProjectedColumnNames(ImmutableList.of("key"))
                .build();

        int maxKey = 0;
        while (scanner.hasMoreRows()) {
            for (RowResult row : scanner.nextRows()) {
                maxKey = Math.max(maxKey, row.getInt("key"));
            }
        }
        scanner.close();
        return maxKey;
    }

    /** Generate a random string */
    static String generateRandomString(int length) {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt(RANDOM.nextInt(chars.length())));
        }
        return sb.toString();
    }
}