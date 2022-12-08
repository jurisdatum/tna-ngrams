package uk.gov.legislation.research.ngrams.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class Documents {

    public static final TableName tableName = TableName.valueOf("documents");
    private static final byte[] fam = Bytes.toBytes("f");
    private static final byte[] yearCol = Bytes.toBytes("year");
    private static final byte[] dateCol = Bytes.toBytes("date");
    private static final byte[] titleCol = Bytes.toBytes("title");
    private static final byte[] statusCol = Bytes.toBytes("status");
    private static final byte[] textCol = Bytes.toBytes("text");

    public static void ensureTableExists(Configuration conf) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        if (admin.tableExists(tableName))
            return;
        TableDescriptorBuilder.ModifyableTableDescriptor tableDescriptor = new TableDescriptorBuilder.ModifyableTableDescriptor(tableName);
        ColumnFamilyDescriptor familyDescriptor = new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(fam);
        tableDescriptor.setColumnFamily(familyDescriptor);
        admin.createTable(tableDescriptor);
        admin.close();
        connection.close();
    }

    public static Put put(String id, short year, Date date, String title) {
        Put put = new Put(Bytes.toBytes(id))
            .addColumn(fam, yearCol, Bytes.toBytes(year));
        if (date != null)
            put = put.addColumn(fam, dateCol, Bytes.toBytes(date.getTime()));
        if (title != null)
            put = put.addColumn(fam, titleCol, Bytes.toBytes(title));
        return put;
    }

    public static Put put(String id, short year, Date date, String title, short status, String text) {
        Put put = new Put(Bytes.toBytes(id))
                .addColumn(fam, yearCol, Bytes.toBytes(year));
        if (date != null)
            put = put.addColumn(fam, dateCol, Bytes.toBytes(date.getTime()));
        if (title != null)
            put = put.addColumn(fam, titleCol, Bytes.toBytes(title));
        if (text != null)
            put = put.addColumn(fam, textCol, Bytes.toBytes(text));
        put = put.addColumn(fam, statusCol, Bytes.toBytes(status));
        return put;
    }

//    public static void put(Connection connection, String id, short year, Date date, String title) throws IOException {
//        Put put = put(id, year, date, title);
//        connection.getTable(tableName).put(put);
//    }
//
//    public static void put(Configuration conf, String id, short year, Date date, String title) throws IOException {
//        Connection connection = ConnectionFactory.createConnection(conf);
//        try {
//            put(connection, id, year, date, title);
//        } finally {
//            connection.close();
//        }
//    }

    public static short getStatus(Result result) {
        return Bytes.toShort(result.getValue(fam, statusCol));
    }
    public static short getYear(Result result) {
        return Bytes.toShort(result.getValue(fam, yearCol));
    }
    public static String getText(Result result) {
        return Bytes.toString(result.getValue(fam, textCol));
    }

    public static Map<String, String> getTitles(Configuration conf, Set<String> ids) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        try {
            ArrayList<Get> gets = new ArrayList<>(ids.size());
            for (String id : ids) {
                Get get = new Get(Bytes.toBytes(id)).addColumn(fam, titleCol);
                gets.add(get);
            }
            Table table = connection.getTable(tableName);
            try {
                LinkedHashMap<String, String> titles = new LinkedHashMap<>();
                Result[] results = table.get(gets);
                for (int i = 0; i < results.length; i++) {
                    Result result = results[i];
                    if (result.getRow() == null) {
                        String key = (String) ids.toArray()[i];
                        titles.put(key, "");
                        continue;
                    }
                    String id = Bytes.toString(result.getRow());
                    String title = Bytes.toString(result.getFamilyMap(fam).get(titleCol));
                    titles.put(id, title);
                }
                return titles;
            } finally {
                table.close();
            }
        } finally {
            connection.close();
        }
    }

}
