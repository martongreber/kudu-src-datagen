import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.SessionConfiguration.FlushMode;

public class DataGen {
    private static final String KUDU_MASTERS = System.getProperty("kuduMasters", "localhost:8764");
    private static final Random RANDOM = new Random();

    static void createExampleTable(KuduClient client, String tableName) throws KuduException {
        // Set up a simple schema.
        List<ColumnSchema> columns = new ArrayList<>(2);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32)
                .key(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING).nullable(true)
                .build());
        Schema schema = new Schema(columns);

        CreateTableOptions cto = new CreateTableOptions();
        List<String> hashKeys = new ArrayList<>(1);
        hashKeys.add("key");
        int numBuckets = 8;
        cto.addHashPartitions(hashKeys, numBuckets);

        // Create the table.
        client.createTable(tableName, schema, cto);
        System.out.println("Created table " + tableName);
    }

    static void insertRowsContinuously(KuduClient client, String tableName) throws KuduException, InterruptedException {
        KuduTable table = client.openTable(tableName);
        KuduSession session = client.newSession();
        session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND);
        int key = 1;

        while (true) {
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            row.addInt("key", key);
            row.addString("value", generateRandomString(5));
            session.apply(insert);

            if (session.countPendingErrors() != 0) {
                System.out.println("Errors inserting row with key " + key);
                org.apache.kudu.client.RowErrorsAndOverflowStatus roStatus = session.getPendingErrors();
                org.apache.kudu.client.RowError[] errs = roStatus.getRowErrors();
                System.out.println("Error details:");
                for (org.apache.kudu.client.RowError err : errs) {
                    System.out.println(err);
                }
            } else {
                System.out.println("Inserted row with key " + key);
            }

            key++;
            Thread.sleep(1000); // Wait for 1 second before inserting the next row
        }
    }

    static String generateRandomString(int length) {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(characters.charAt(RANDOM.nextInt(characters.length())));
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        System.out.println("-----------------------------------------------");
        System.out.println("Will try to connect to Kudu master(s) at " + KUDU_MASTERS);
        System.out.println("Run with -DkuduMasters=master-0:port,master-1:port,... to override.");
        System.out.println("-----------------------------------------------");
        String tableName = "test_table";
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();

        try {
            // Check if the table exists
            if (client.tableExists(tableName)) {
                System.out.println("Table " + tableName + " already exists. Deleting...");
                client.deleteTable(tableName);
                System.out.println("Deleted table " + tableName);
            }

            // Create the table and start inserting data
            createExampleTable(client, tableName);
            insertRowsContinuously(client, tableName);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                client.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}