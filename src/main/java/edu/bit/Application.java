package edu.bit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * This is the place to start off the track of a bit of anything.
 */
public class Application {

    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        String path = Application.class.getClassLoader()
                .getResource("hbase-site.xml")
                .getPath();
        config.addResource(new Path(path));
        Connection connection = ConnectionFactory.createConnection(config);
        try {
            Admin admin = connection.getAdmin();
            System.out.println(">>>" + admin);
            createTable(admin);
            putRow(connection.getTable(TABLE_NAME));
            System.out.println(">>> Successfully Created the table and inserted a row " + admin);
            Result result = connection.getTable(TABLE_NAME).get(new Get(ROW_ID));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static final TableName TABLE_NAME = TableName.valueOf("test");
    private static final byte[] CF_NAME = Bytes.toBytes("hb");
    private static final byte[] QUALIFIER = Bytes.toBytes("cl");
    private static final byte[] ROW_ID = Bytes.toBytes("rw");

    public static void createTable(final Admin admin) throws IOException {
        if (!admin.tableExists(TABLE_NAME)) {
            TableDescriptor desc = TableDescriptorBuilder.newBuilder(TABLE_NAME)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_NAME))
                    .build();
            admin.createTable(desc);
        }
    }

    public static void putRow(final Table table) throws IOException {
        table.put(new Put(ROW_ID).addColumn(CF_NAME, QUALIFIER, Bytes.toBytes("Hello, World !")));
    }
}