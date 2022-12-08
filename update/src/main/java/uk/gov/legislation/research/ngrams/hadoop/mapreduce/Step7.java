package uk.gov.legislation.research.ngrams.hadoop.mapreduce;

import uk.gov.legislation.research.Legislation;
import uk.gov.legislation.research.ngrams.Ngrams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import uk.gov.legislation.research.ngrams.Search;
import uk.gov.legislation.research.ngrams.hadoop.hbase.Counts;
import uk.gov.legislation.research.ngrams.hadoop.hbase.Search1;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class Step7 {

    /* populates 'search' table */

    private static final int Limit = 100;

    private static final Logger logger = Logger.getLogger(Step7.class.getName());

    public static boolean run(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        logger.info("beginning step 7");
        Search1.ensureTableExists(conf);
        Job job = Job.getInstance(conf);
        job.setJobName("step 7: write search data to HBase");
        job.setJarByClass(Step7.class);
        TableMapReduceUtil.initTableMapperJob(Counts.tableName, new Scan(), Mapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob(Search1.tableName.getNameAsString(), Reducer.class, job);
        return job.waitForCompletion(true);
    }

    public static class Mapper extends TableMapper<Text, Text> {

        public void map(ImmutableBytesWritable row, Result record, Context context) throws IOException, InterruptedException {
            Counts.Key key = Counts.Key.parse(row.get());
            Set<String> components = Search.components(key.ngram);
//            logger.info("mapping " + key.ngram + ": " + components.size() + " components");
            if (components.isEmpty())
                return;
            short n = (short) key.ngram.split(" ").length;
            int total = Counts.extractCounts(record).values().stream().mapToInt(i -> i.intValue()).sum();
            for (String component : components) {
//                Text keyOut = new Text(component + "|" + key.legType.name() + "|" + HBase.toShortString(key.ngType) + "|" + n);
//                Text keyOut = new Text(makeIntermediateKey(component, key.legType, key.ngType, n));
                Text keyOut = new Text(new Key(component, key.legType, key.ngType, n).toString());
                Text valOut = new Text(key.ngram + "|" + total);
                context.write(keyOut, valOut);
            }
        }

    }

//    private static String makeIntermediateKey(String component, Legislation.Type legType, Ngrams.Type ngType, int n) {
//        return component + "|" + legType.name() + "|" + HBase.toShortString(ngType) + "|" + n;
//    }
//    private static Object[] parseIntermediateKey(String key) {
//        int i3 = key.lastIndexOf('|');
//        int i2 = key.lastIndexOf('|', i3 -1);
//        int i1 = key.lastIndexOf('|', i2 -1);
//        return new Object[] {
//            key.substring(0, i1),
//            Legislation.Type.valueOf(key.substring(i1 + 1, i2)),
//            HBase.fromShortString(key.substring(i2 + 1, i3)),
//            Integer.parseInt(key.substring(i3 + 1))
//        };
//    }

    static class Key {
        private final String s;
        final String component;
        final Legislation.Searchable legType;
        final Ngrams.Type ngType;
        final short n;
        Key(String key) {
            s = key;
            int i3 = key.lastIndexOf('|');
            int i2 = key.lastIndexOf('|', i3 - 1);
            int i1 = key.lastIndexOf('|', i2 - 1);
            component = key.substring(0, i1);
            Legislation.Searchable legType1;
            try {
                legType1 = Legislation.Type.valueOf(key.substring(i1 + 1, i2));
            } catch (IllegalArgumentException e) {
                legType1 = Legislation.Group.valueOf(key.substring(i1 + 1, i2));
            }
            legType = legType1;
            ngType = Counts.fromShortString(key.substring(i2 + 1, i3));
            n = Short.parseShort(key.substring(i3 + 1));
        }
        Key(String component, Legislation.Searchable legType, Ngrams.Type ngType, short n) {
            s = component + "|" + legType.name() + "|" + Counts.toShortString(ngType) + "|" + n;
            this.component = component;
            this.legType = legType;
            this.ngType = ngType;
            this.n = n;
        }
        @Override
        public String toString() {
            return s;
        }
    }

    static class Value implements Comparable<Value> {
        private final String string;
        final String ngram;
        final int total;
        Value(String value) {
            string = value;
            int i = value.lastIndexOf('|');
            ngram = value.substring(0, i);
            total = Integer.parseInt(value.substring(i + 1));
        }
        Value(String ngram, int total) {
            string = ngram + "|" + total;
            this.ngram = ngram;
            this.total = total;
        }
        @Override
        public String toString() {
            return string;
        }
        @Override
        public int compareTo(Value o) {
            return Integer.compare(total, o.total);
        }
    }

    public static class Reducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            Object[] parts = parseIntermediateKey(key.toString());
//            String component = (String) parts[0];
//            Legislation.Type legType = (Legislation.Type) parts[1];
//            Ngrams.Type ngType = (Ngrams.Type) parts[2];
//            short n = ((Integer) parts[3]).shortValue();
            Key parsedKey = new Key(key.toString());
            String component = parsedKey.component;
            Legislation.Searchable legType = parsedKey.legType;
            Ngrams.Type ngType = parsedKey.ngType;
            short n = parsedKey.n;
//            LinkedHashMap<String, Integer> pairs = StreamSupport.stream(values.spliterator(), false)
//                .map(text -> text.toString())
//                .map(line -> { int i = line.lastIndexOf('|'); return new Pair<String, Integer>(line.substring(0, i), Integer.parseInt(line.substring(i + 1))); })
//                .sorted((a, b) -> b.getSecond().compareTo(a.getSecond()))
//                .limit(Limit)
//                .collect(Collectors.toMap(p -> p.getFirst(), p -> p.getSecond(), (x, y) -> y, LinkedHashMap::new));
            PriorityQueue<Value> queue = new PriorityQueue<Value>(Limit + 1);   // , (v1, v2) -> Integer.compare(v1.total, v2.total)
            PriorityQueue<Value> startsWith = new PriorityQueue<Value>(Limit + 1);   // , (v1, v2) -> Integer.compare(v1.total, v2.total)
            for (Text text : values) {
                Value val = new Value(text.toString());
                queue.add(val);
                if (queue.size() > Limit)
                    queue.poll();
                if (text.toString().startsWith(component)) {
                    startsWith.add(val);
                    if (startsWith.size() > Limit)
                        startsWith.poll();
                }
            }
            LinkedHashMap<String, Integer> pairs = queue.stream()
                .sorted((v1, v2) -> Integer.compare(v2.total, v1.total))
                .collect(Collectors.toMap(v -> v.ngram, v -> v.total, (x, y) -> y, LinkedHashMap::new));
            LinkedHashMap<String, Integer> startsWithPairs = startsWith.stream()
                    .sorted((v1, v2) -> Integer.compare(v2.total, v1.total))
                    .collect(Collectors.toMap(v -> v.ngram, v -> v.total, (x, y) -> y, LinkedHashMap::new));
            Put put = Search1.put(legType, component, ngType, n, pairs, startsWithPairs);
            context.write(new ImmutableBytesWritable(put.getRow()), put);
        }

    }

}
