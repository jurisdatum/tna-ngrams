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

public class Counts {

    public static final TableName tableName = TableName.valueOf("counts");
    private static final byte[] countsFamily = Bytes.toBytes("c");
    private static final byte[] docsFamily = Bytes.toBytes("d");

    public static void ensureTableExists(Configuration conf) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        if (admin.tableExists(tableName))
            return;
        TableDescriptorBuilder.ModifyableTableDescriptor tableDescriptor = new TableDescriptorBuilder.ModifyableTableDescriptor(tableName);
        ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor countsFamilyDescriptor = new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(countsFamily);
        countsFamilyDescriptor.setMaxVersions(1);
        tableDescriptor.setColumnFamily(countsFamilyDescriptor);
        ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor docsFamilyDescriptor = new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(docsFamily);
        docsFamilyDescriptor.setMaxVersions(1);
        tableDescriptor.setColumnFamily(docsFamilyDescriptor);
        admin.createTable(tableDescriptor);
        admin.close();
        connection.close();
    }

    public static String toShortString(Ngrams.Type ngType) {
        switch (ngType) {
            case Case_Sensitive:
                return "s";
            case Case_Insensitive:
                return "i";
            case Lemmas:
                return "l";
            default:
                throw new RuntimeException();
        }
    }
    public static Ngrams.Type fromShortString(String s) {
        switch (s) {
            case "s":
                return Ngrams.Type.Case_Sensitive;
            case "i":
                return Ngrams.Type.Case_Insensitive;
            case "l":
                return Ngrams.Type.Lemmas;
            default:
                throw new RuntimeException();
        }
    }

    public static class Key {
        public final Legislation.Searchable legType;
        public final Ngrams.Type ngType;
        public final String ngram;
        public Key(Legislation.Searchable legType, Ngrams.Type ngType, String ngram) {
            this.legType = legType;
            this.ngType = ngType;
            this.ngram = ngram;
        }
        public static Key parse(String key) {
            int i2 = key.lastIndexOf('|');
            int i1 = key.lastIndexOf('|', i2 - 1);
            String ngram = key.substring(0, i1);
            Legislation.Searchable legType;
            try {
                legType = Legislation.Type.valueOf(key.substring(i1 + 1, i2));
            } catch (IllegalArgumentException e) {
                legType = Legislation.Group.valueOf(key.substring(i1 + 1, i2));
            }
            Ngrams.Type ngType = fromShortString(key.substring(i2 + 1));
            return new Key(legType, ngType, ngram);
        }
        public static Key parse(byte[] row) {
            String key = Bytes.toString(row);
            return parse(key);
        }
        @Override
        public String toString() {
            return ngram + "|" + legType.name() + "|" + toShortString(ngType);
        }
    }

    private static String serialize(LinkedHashMap<String, Integer> counts) {
        return counts.entrySet().stream()
            .map(entry -> entry.getKey() + ":" + entry.getValue())
            .collect(Collectors.joining(","));
    }
    private static LinkedHashMap<String, Integer> deserialize(String counts) {
        return Arrays.stream(counts.split(","))
            .map(s -> s.split(":", 2))
            .collect(Collectors.toMap(a -> a[0], a -> Integer.parseInt(a[1]), (x, y) -> y, LinkedHashMap::new));
    }

    public static Put put(Legislation.Searchable legType, short year, Ngrams.Type ngType, String ngram, LinkedHashMap<String, Integer> docCounts, int sum) {
        String key = new Key(legType, ngType, ngram).toString();
        byte[] row = Bytes.toBytes(key);
        byte[] col = Bytes.toBytes(year);
        byte[] sumVal = Bytes.toBytes(sum);
        byte[] docCountsVal = Bytes.toBytes(serialize(docCounts));
        return new Put(row)
            .addColumn(countsFamily, col, sumVal)
            .addColumn(docsFamily, col, docCountsVal);
    }

    public static Put put(Legislation.Searchable legType, Ngrams.Type ngType, String ngram, Map<Short, Integer> yearCounts, Map<Short, LinkedHashMap<String, Integer>> docCounts) {
//        byte[] row = Bytes.toBytes(makeKey(legType, ngType, ngram));
        String key = new Key(legType, ngType, ngram).toString();
        byte[] row = Bytes.toBytes(key);
        Put put = new Put(row);
        for (Map.Entry<Short, Integer> entry : yearCounts.entrySet()) {
            byte[] col = Bytes.toBytes(entry.getKey());
            byte[] val = Bytes.toBytes(entry.getValue());
            put.addColumn(countsFamily, col, val);
        }
        for (Map.Entry<Short, LinkedHashMap<String, Integer>> entry : docCounts.entrySet()) {
            byte[] col = Bytes.toBytes(entry.getKey());
            byte[] val = Bytes.toBytes(serialize(entry.getValue()));
            put.addColumn(docsFamily, col, val);
        }
        return put;
    }




    public static Result getResult(Connection connection, String key) throws IOException {
        byte[] row = Bytes.toBytes(key);
        Table table = connection.getTable(tableName);
        Get get = new Get(row);
        return table.get(get);
    }

    private static Get getCounts(Legislation.Searchable legType, String ngram, Ngrams.Type ngType) {
        String key = new Key(legType, ngType, ngram).toString();
        byte[] row = Bytes.toBytes(key);
        return new Get(row).addFamily(countsFamily);
    }

    private static NavigableMap<Short, Integer> convert(NavigableMap<byte[], byte[]> counts1) {
        NavigableMap<Short, Integer> counts2 = new TreeMap<>();
        for (Map.Entry<byte[], byte[]> entry : counts1.entrySet()) {
            Short year = Bytes.toShort(entry.getKey());
            Integer count = Bytes.toInt(entry.getValue());
            counts2.put(year, count);
        }
        return counts2;
    }
    public static NavigableMap<Short, Integer> extractCounts(Result result) {
        NavigableMap<byte[], byte[]> counts1 = result.getFamilyMap(countsFamily);
        return convert(counts1);
    }

    public static LinkedHashMap<String, NavigableMap<Short, Integer>> getCounts(Configuration conf, Legislation.Searchable legType, LinkedHashSet<String> ngrams, Ngrams.Type ngType) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        try {
            ArrayList<Get> gets = new ArrayList<>(ngrams.size());
            for (String ngram : ngrams)
                gets.add(getCounts(legType, ngram, ngType));
            Table table = connection.getTable(tableName);
            try {
                LinkedHashMap<String, NavigableMap<Short, Integer>> counts = new LinkedHashMap<>();
                Result[] results = table.get(gets);
                for (int i = 0; i < results.length; i++) {
                    Result result = results[i];
                    if (result.getRow() == null) {
                        String key = (String) ngrams.toArray()[i];
                        counts.put(key, new TreeMap<>());
                        continue;
                    }
                    Key key = Key.parse(result.getRow());
                    counts.put(key.ngram, extractCounts(result));
                }
                return counts;
            } finally {
                table.close();
            }
        } finally {
            connection.close();
        }
    }

    /* get document counts */

    private static Get getDocuments(Legislation.Searchable legType, String ngram, Ngrams.Type ngType) {
        String key = new Key(legType, ngType, ngram).toString();
        byte[] row = Bytes.toBytes(key);
        return new Get(row).addFamily(docsFamily);
    }

    private static TreeMap<Short, LinkedHashMap<String, Integer>> convertDocumentCounts(NavigableMap<byte[], byte[]> counts1) {
        TreeMap<Short, LinkedHashMap<String, Integer>> allYears = new TreeMap<>();
        for (Map.Entry<byte[], byte[]> entry : counts1.entrySet()) {
            Short year = Bytes.toShort(entry.getKey());
            String serialized = Bytes.toString(entry.getValue());
            LinkedHashMap<String, Integer> deserialized = deserialize(serialized);
            allYears.put(year, deserialized);
        }
        return allYears;
    }

    public static TreeMap<Short, LinkedHashMap<String, Integer>> extractDocumentCounts(Result result) {
        NavigableMap<byte[], byte[]> raw = result.getFamilyMap(docsFamily);
        return convertDocumentCounts(raw);
    }
    public static LinkedHashMap<String, NavigableMap<Short, LinkedHashMap<String, Integer>>> getDocumentCounts(Configuration conf, Legislation.Searchable legType, LinkedHashSet<String> ngrams, Ngrams.Type ngType) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        try {
            ArrayList<Get> gets = new ArrayList<>(ngrams.size());
            for (String ngram : ngrams)
                gets.add(getDocuments(legType, ngram, ngType));
            Table table = connection.getTable(tableName);
            try {
                LinkedHashMap<String, NavigableMap<Short, LinkedHashMap<String, Integer>>> counts = new LinkedHashMap<>();
                Result[] results = table.get(gets);
                for (int i = 0; i < results.length; i++) {
                    Result result = results[i];
                    if (result.getRow() == null) {
                        String key = (String) ngrams.toArray()[i];
                        counts.put(key, new TreeMap<>());
                        continue;
                    }
                    Key key = Key.parse(result.getRow());
                    counts.put(key.ngram, extractDocumentCounts(result));
                }
                return counts;
            } finally {
                table.close();
            }
        } finally {
            connection.close();
        }
    }

    public static LinkedHashMap<String, LinkedHashMap<String, Integer>> getDocumentCounts(Configuration conf, Legislation.Searchable legType, LinkedHashSet<String> ngrams, Ngrams.Type ngType, short year) throws IOException {
        /* optimized for a single year */
        throw new UnsupportedOperationException();
    }

}
