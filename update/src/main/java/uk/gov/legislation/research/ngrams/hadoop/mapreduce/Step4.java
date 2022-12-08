package uk.gov.legislation.research.ngrams.hadoop.mapreduce;

import uk.gov.legislation.research.Legislation;
import uk.gov.legislation.research.ngrams.Globals;
import uk.gov.legislation.research.ngrams.Ngrams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import uk.gov.legislation.research.ngrams.hadoop.hbase.Counts;
import uk.gov.legislation.research.ngrams.hadoop.hbase.Documents;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class Step4 {

    private static final Logger logger = Logger.getLogger(Step4.class.getName());

    public static boolean run(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        logger.info("beginning step 4");
        Counts.ensureTableExists(conf);
        Job job = Job.getInstance(conf);
        job.setJobName("step 4: write counts to HBase");
        job.setJarByClass(Step4.class);
        TableMapReduceUtil.initTableMapperJob(Documents.tableName, new Scan(), Mapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob(Counts.tableName.getNameAsString(), Reducer.class, job);
        return job.waitForCompletion(true);
    }

    public static class Mapper extends TableMapper<Text, Text> {

        public void map(ImmutableBytesWritable row, Result record, Context context) throws IOException, InterruptedException {
//            boolean test = context.getConfiguration().getBoolean(Update.TestPropertyName, false);
            String id = Bytes.toString(row.get());
//            short status = Documents.getStatus(record);
//            if (status != 200)
//                return;

            Legislation.Type legType = Legislation.Type.valueOf(id.substring(0, id.indexOf('/')));
//            short year = Documents.getYear(record);
//
//            logger.info("mapping: " + id);
//
//            String text = Documents.getText(record);
//            Ngrams ngrams = new Ngrams(text);
//
//            Text outKey = new Text();
//            Text outVal = new Text();
//            for (int n = 1; n <= Globals.MAX_N; n++) {
//                if (test && n > 2)
//                    continue;
//                for (Ngrams.Type ngType : Ngrams.Type.values()) {
//                    if (test && !ngType.equals(Ngrams.Type.Case_Sensitive))
//                        continue;
//                    Map<String, Integer> ng = ngrams.get(n, ngType);
//                    for (Map.Entry<String, Integer> counts : ng.entrySet()) {
//                        String ngram = counts.getKey();
//                        if (test && !ngram.startsWith("a"))
//                            continue;
//                        Integer count = counts.getValue();
//                        String outKey1 = legType.name() + "|" + year + "|" + Counts.toShortString(ngType) + "|" + ngram;
//                        String outVal1 = id + ":" + count;
//                        outKey.set(outKey1);
//                        outVal.set(outVal1);
//                        context.write(outKey, outVal);
//                    }
//                }
//            }
            map2(record, context, id, legType);
        }

        static void map2(Result record, Context context, String id, Legislation.Searchable legType) throws IOException, InterruptedException {
            boolean test = context.getConfiguration().getBoolean(Update.TestPropertyName, false);
            short status = Documents.getStatus(record);
            if (status != 200)
                return;
            short year = Documents.getYear(record);
            String text = Documents.getText(record);

            Ngrams ngrams = new Ngrams(text);

            Text outKey = new Text();
            Text outVal = new Text();
            for (int n = 1; n <= Globals.MAX_N; n++) {
                if (test && n > 2)
                    continue;
                for (Ngrams.Type ngType : Ngrams.Type.values()) {
                    if (test && !ngType.equals(Ngrams.Type.Case_Sensitive))
                        continue;
                    Map<String, Integer> ng = ngrams.get(n, ngType);
                    for (Map.Entry<String, Integer> counts : ng.entrySet()) {
                        String ngram = counts.getKey();
                        if (test && !ngram.startsWith("a"))
                            continue;
                        Integer count = counts.getValue();
                        String outKey1 = legType.name() + "|" + year + "|" + Counts.toShortString(ngType) + "|" + ngram;
                        String outVal1 = id + ":" + count;
                        outKey.set(outKey1);
                        outVal.set(outVal1);
                        context.write(outKey, outVal);
                    }
                }
            }
        }

    }

    static class Reducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] keyParts = key.toString().split("\\|", 4);
            Legislation.Type legType = Legislation.Type.valueOf(keyParts[0]);
            short year = Short.parseShort(keyParts[1]);
            Ngrams.Type ngType = Counts.fromShortString(keyParts[2]);
            String ngram = keyParts[3];
////            logger.info("reducing: " + key.toString());
//            Map<String, Integer> counts = new HashMap<>();
//            int sum = 0;
//            for (Text value : values) {
////                String[] valueParts = value.toString().split(":", 2);
////                String id = valueParts[0];
////                int count = Integer.parseInt(valueParts[1]);
//                int colon = value.toString().indexOf(':');
//                String id = value.toString().substring(0, colon);
//                int count = Integer.parseInt(value.toString().substring(colon + 1));
//                counts.put(id, count);
//                sum += count;
//            }
//            LinkedHashMap<String, Integer> counts2 = orderByValue(counts);
//            Put put = Counts.put(legType, year, ngType, ngram, counts2, sum);
//            ImmutableBytesWritable keyOut = new ImmutableBytesWritable(put.getRow());
//            context.write(keyOut, put);
            reduce2(context, values, legType, year, ngType, ngram);
        }

        static void reduce2(Context context, Iterable<Text> values, Legislation.Searchable legType, short year, Ngrams.Type ngType, String ngram) throws IOException, InterruptedException {
            Map<String, Integer> counts = new HashMap<>();
            int sum = 0;
            for (Text value : values) {
                int colon = value.toString().indexOf(':');
                String id = value.toString().substring(0, colon);
                int count = Integer.parseInt(value.toString().substring(colon + 1));
                counts.put(id, count);
                sum += count;
            }
            LinkedHashMap<String, Integer> counts2 = orderByValue(counts);
            Put put = Counts.put(legType, year, ngType, ngram, counts2, sum);
            ImmutableBytesWritable keyOut = new ImmutableBytesWritable(put.getRow());
            context.write(keyOut, put);
        }

    }

    public static LinkedHashMap<String, Integer> orderByValue(Map<String, Integer> map) {
        return map.entrySet().stream()
            .sorted((e1, e2) -> Integer.compare(e1.getValue(), e2.getValue()) * -1)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (x, y) -> y, LinkedHashMap::new));
    }

}
