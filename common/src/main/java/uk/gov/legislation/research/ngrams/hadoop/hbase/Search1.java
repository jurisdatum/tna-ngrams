package uk.gov.legislation.research.ngrams.hadoop.hbase;

import uk.gov.legislation.research.Legislation;
import uk.gov.legislation.research.ngrams.Ngrams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.gson.stream.JsonReader;
import org.apache.hbase.thirdparty.com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.*;

public class Search1 {

    public static final TableName tableName = TableName.valueOf("search");

    private static byte[] allFam = Bytes.toBytes("n");
    private static byte[] startsWithFam = Bytes.toBytes("s");

    public static void ensureTableExists(Configuration conf) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        if (admin.tableExists(tableName))
            return;
        TableDescriptorBuilder.ModifyableTableDescriptor tableDescriptor = new TableDescriptorBuilder.ModifyableTableDescriptor(tableName);
        ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor familyDescriptor1 = new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(allFam);
        familyDescriptor1.setMaxVersions(1);
        tableDescriptor.setColumnFamily(familyDescriptor1);
        ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor familyDescriptor2 = new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(startsWithFam);
        familyDescriptor2.setMaxVersions(1);
        tableDescriptor.setColumnFamily(familyDescriptor2);
        admin.createTable(tableDescriptor);
        admin.close();
        connection.close();
    }

    private static String serialize(LinkedHashMap<String, Integer> counts) throws IOException {
        StringWriter writer = new StringWriter();
        JsonWriter json = new JsonWriter(writer);
        json.beginObject();
        for (Map.Entry<String, Integer> entry : counts.entrySet())
            json.name(entry.getKey()).value(entry.getValue());
        json.endObject();
        return writer.toString();
    }

//    public static Put put(Legislation.Searchable legType, String component, Ngrams.Type ngType, short n, LinkedHashMap<String, Integer> ngrams) throws IOException {
//        byte[] row = Bytes.toBytes(new Counts.Key(legType, ngType, component).toString());
//        byte[] col = new byte[] { (byte) n };
//        byte[] docCountsVal = Bytes.toBytes(serialize(ngrams));
//        return new Put(row).addColumn(fam, col, docCountsVal);
//    }
    public static Put put(Legislation.Searchable legType, String component, Ngrams.Type ngType, short n, LinkedHashMap<String, Integer> ngrams, LinkedHashMap<String, Integer> ngramsStartingWith) throws IOException {
        byte[] row = Bytes.toBytes(new Counts.Key(legType, ngType, component).toString());
        byte[] col = new byte[] { (byte) n };
        byte[] docCountsVal = Bytes.toBytes(serialize(ngrams));
        byte[] startsWithVal = Bytes.toBytes(serialize(ngramsStartingWith));
        return new Put(row).addColumn(allFam, col, docCountsVal).addColumn(startsWithFam, col, startsWithVal);
    }

    /* get */

    private static LinkedHashMap<String, Long> deserialize(String counts) throws IOException {
        LinkedHashMap<String, Long> map = new LinkedHashMap<>();
        Reader reader = new StringReader(counts);
        JsonReader json = new JsonReader(reader);
        json.beginObject();
        while (json.hasNext()) {
            String key = json.nextName();
            long value = json.nextLong();
            map.put(key, value);
        }
        json.endObject();
        json.close();
        reader.close();
        return map;
    }

    private static TreeMap<Integer, LinkedHashMap<String, Long>> extract(Result result, boolean beginning) throws IOException {
        byte[] fam = beginning ? startsWithFam : allFam;
        TreeMap<Integer, LinkedHashMap<String, Long>> map = new TreeMap<>();
        NavigableMap<byte[], byte[]> raw = result.getFamilyMap(fam);
        for (Map.Entry<byte[], byte[]> raw1 : raw.entrySet()) {
            short key = raw1.getKey()[0];
            LinkedHashMap<String, Long> value = deserialize(Bytes.toString(raw1.getValue()));
            map.put((int) key, value);
        }
        return map;
    }

    public static SortedMap<Integer, LinkedHashMap<String, Long>> get(Configuration conf, Legislation.Searchable legType, Set<String> components, Ngrams.Type ngType, boolean beginning) throws IOException {
        byte[] fam = beginning ? startsWithFam : allFam;
        Connection connection = ConnectionFactory.createConnection(conf);
        try {
            ArrayList<Get> gets = new ArrayList<>(components.size());
            for (String component : components) {
                Get get = new Get(Bytes.toBytes(new Counts.Key(legType, ngType, component).toString())).addFamily(fam);
                gets.add(get);
            }
            Table table = connection.getTable(tableName);
            try {
                SortedMap<Integer, LinkedHashMap<String, Long>> ngrams = null;
                Result[] results = table.get(gets);
                for (int i = 0; i < results.length; i++) {
                    Result result = results[i];
                    if (result.getRow() == null)
                        return new TreeMap<>();
                    SortedMap<Integer, LinkedHashMap<String, Long>> values = extract(result, beginning);
                    if (ngrams == null) {
                        ngrams = values;
                        continue;
                    }
                    for (Integer n : ngrams.keySet()) {
                        LinkedHashMap<String, Long> oldValues = ngrams.getOrDefault(n, new LinkedHashMap<>());
                        LinkedHashMap<String, Long> newValues = values.getOrDefault(n, new LinkedHashMap<>());
                        for (String ngram : oldValues.keySet())
                            if (!newValues.containsKey(ngram))
                                oldValues.remove(ngram);
                    }
                }
                return ngrams;
            } finally {
                table.close();
            }
        } finally {
            connection.close();
        }
    }

}
