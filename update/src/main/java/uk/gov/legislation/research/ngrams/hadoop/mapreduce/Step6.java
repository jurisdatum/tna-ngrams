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
import uk.gov.legislation.research.ngrams.hadoop.hbase.Counts;
import uk.gov.legislation.research.ngrams.hadoop.hbase.Scales;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public class Step6 {

    /* computes scaling factors */

    private static final Logger logger = Logger.getLogger(Step6.class.getName());

    public static boolean run(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        logger.info("beginning step 6");
        Scales.ensureTableExists(conf);
        Job job = Job.getInstance(conf);
        job.setJobName("step 6: generate scaling factors");
        job.setJarByClass(Step6.class);
        TableMapReduceUtil.initTableMapperJob(Counts.tableName, new Scan(), Mapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob(Scales.tableName.getNameAsString(), Reducer.class, job);
        return job.waitForCompletion(true);
    }

    public static class Mapper extends TableMapper<Text, Text> {

        public void map(ImmutableBytesWritable row, Result record, Context context) throws IOException, InterruptedException {
            Counts.Key keyIn = Counts.Key.parse(row.get());
            short n = (short) keyIn.ngram.split(" ").length;
            NavigableMap<Short, Integer> counts = Counts.extractCounts(record);
            Text keyOut = new Text();
            Text valOut = new Text();
            for (Map.Entry<Short, Integer> entry : counts.entrySet()) {
                keyOut.set(new Key(keyIn.legType, keyIn.ngType, n).toString());
                valOut.set(new Value(entry.getKey(), entry.getValue()).toString());
                context.write(keyOut, valOut);
            }
        }

    }

    static class Key {
        private final String s;
        final Legislation.Searchable legType;
        final Ngrams.Type ngType;
        final short n;
        Key(String key) {
            s = key;
            int i2 = key.lastIndexOf('|');
            int i1 = key.lastIndexOf('|', i2 -1);
            Legislation.Searchable legType1;
            try {
                legType1 = Legislation.Type.valueOf(key.substring(0, i1));
            } catch (IllegalArgumentException e) {
                legType1 = Legislation.Group.valueOf(key.substring(0, i1));
            }
            legType = legType1;
            ngType = Counts.fromShortString(key.substring(i1 + 1, i2));
            n = Short.parseShort(key.substring(i2 + 1));
        }
        Key(Legislation.Searchable legType, Ngrams.Type ngType, short n) {
            s = legType.name() + "|" + Counts.toShortString(ngType) + "|" + n;
            this.legType = legType;
            this.ngType = ngType;
            this.n = n;
        }
        @Override
        public String toString() {
            return s;
        }
    }

    static class Value {
        private final String string;
        final short year;
        final int total;
        Value(String value) {
            string = value;
            int i = value.lastIndexOf('|');
            year = Short.parseShort(value.substring(0, i));
            total = Integer.parseInt(value.substring(i + 1));
        }
        Value(short year, int total) {
            string = year + "|" + total;
            this.year = year;
            this.total = total;
        }
        @Override
        public String toString() {
            return string;
        }
    }



    public static class Reducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Key keyIn = new Key(key.toString());

            NavigableMap<Short, AtomicLong> totals = new TreeMap<>();   // yearly totals, may of year to total
            for (Text text : values) {
                Value val = new Value(text.toString());
                if (!totals.containsKey(val.year))
                    totals.put(val.year, new AtomicLong());
                totals.get(val.year).addAndGet(val.total);
            }

            int numberOfYears = totals.size();
            long grandTotal = totals.values().stream().mapToLong(p -> p.get()).sum();
            double scale = (double) numberOfYears / grandTotal;
            logger.info("new way scale (for " + key.toString() + ") = " + scale);

            TreeMap<Short, Double> scales = new TreeMap<>();
            for (Map.Entry<Short, AtomicLong> x : totals.entrySet())
                scales.put(x.getKey(), scale * x.getValue().get());

            Put put = Scales.put(keyIn.legType, keyIn.ngType, keyIn.n, scales);
            context.write(new ImmutableBytesWritable(put.getRow()), put);
        }

    }

}
