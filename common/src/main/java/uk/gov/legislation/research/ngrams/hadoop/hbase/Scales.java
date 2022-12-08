package uk.gov.legislation.research.ngrams.hadoop.hbase;

import uk.gov.legislation.research.Legislation;
import uk.gov.legislation.research.ngrams.Ngrams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class Scales {

    public static final TableName tableName = TableName.valueOf("scales");

    private static byte[] fam = Bytes.toBytes("s");

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

    private static String makeKey(Legislation.Searchable legType, Ngrams.Type ngType) {
        return legType.name() + "|" + Counts.toShortString(ngType);
    }

    private static String serialize(Map<Short, Double> scales) {
        return scales.entrySet().stream()
            .map(entry -> entry.getKey() + ":" + entry.getValue())
            .collect(Collectors.joining(","));
    }
    private static Map<Short, Double> deserialize(String scales) {
        return Arrays.stream(scales.split(","))
            .map(s -> s.split(":", 2))
            .collect(Collectors.toMap(a -> Short.parseShort(a[0]), a -> Double.parseDouble(a[1])));
    }

    public static Put put(Legislation.Searchable legType, Ngrams.Type ngType, short n, SortedMap<Short, Double> scales) {
        byte[] row = Bytes.toBytes(makeKey(legType, ngType));
        byte[] col = new byte[] { (byte) n };
        byte[] val = Bytes.toBytes(serialize(scales));
        return new Put(row).addColumn(fam, col, val);
    }

    public static ArrayList<Map<Short, Double>> get(Configuration conf, Legislation.Type legType, Ngrams.Type ngType) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        try {
            Table table = connection.getTable(tableName);
            byte[] row = Bytes.toBytes(makeKey(legType, ngType));
            Get get = new Get(row).addFamily(fam);
            Result result = table.get(get);
            NavigableMap<byte[], byte[]> scales1 = result.getFamilyMap(fam);
            return convert(scales1);
        } finally {
            connection.close();
        }
    }

    private static ArrayList<Map<Short, Double>> convert(NavigableMap<byte[], byte[]> scales1) {
        NavigableMap<Short, Map<Short, Double>> scales2 = new TreeMap<>();
        for (Map.Entry<byte[], byte[]> entry : scales1.entrySet()) {
            Short n = (short) entry.getKey()[0];
            Map<Short, Double> scales = deserialize(Bytes.toString(entry.getValue()));
            scales2.put(n, scales);
        }
        ArrayList<Map<Short, Double>> list = new ArrayList<>();
        for (Map<Short, Double> map : scales2.values())
            list.add(map);
        return list;
    }

}
