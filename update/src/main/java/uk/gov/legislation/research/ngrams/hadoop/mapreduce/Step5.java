package uk.gov.legislation.research.ngrams.hadoop.mapreduce;

import uk.gov.legislation.research.Legislation;
import uk.gov.legislation.research.ngrams.Ngrams;
import org.apache.hadoop.conf.Configuration;
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

public class Step5 {

    private static final Logger logger = Logger.getLogger(Step5.class.getName());

    public static boolean run(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        logger.info("beginning step 5");

        Job job1 = Job.getInstance(conf);
        job1.setJobName("step 5.1: write counts for primary/secondary groups");
        job1.setJarByClass(Step5.class);
        TableMapReduceUtil.initTableMapperJob(Documents.tableName, new Scan(), Mapper1.class, Text.class, Text.class, job1);
        TableMapReduceUtil.initTableReducerJob(Counts.tableName.getNameAsString(), Reducer.class, job1);
        if (!job1.waitForCompletion(true))
            return false;

        Job job2 = Job.getInstance(conf);
        job2.setJobName("step 5.2: write counts for uk/eu groups");
        job2.setJarByClass(Step5.class);
        TableMapReduceUtil.initTableMapperJob(Documents.tableName, new Scan(), Mapper2.class, Text.class, Text.class, job2);
        TableMapReduceUtil.initTableReducerJob(Counts.tableName.getNameAsString(), Reducer.class, job2);
        if (!job2.waitForCompletion(true))
            return false;

        Job job3 = Job.getInstance(conf);
        job3.setJobName("step 5.3: write counts for 'all' group");
        job3.setJarByClass(Step5.class);
        TableMapReduceUtil.initTableMapperJob(Documents.tableName, new Scan(), Mapper3.class, Text.class, Text.class, job3);
        TableMapReduceUtil.initTableReducerJob(Counts.tableName.getNameAsString(), Reducer.class, job3);
        return job3.waitForCompletion(true);
    }

    public static class Mapper1 extends TableMapper<Text, Text> {

        public void map(ImmutableBytesWritable row, Result record, Context context) throws IOException, InterruptedException {
            String id = Bytes.toString(row.get());
            Legislation.Type legType = Legislation.Type.valueOf(id.substring(0, id.indexOf('/')));
            Legislation.Group legGroup;
            if (Legislation.Group.primary.types().contains(legType))
                legGroup = Legislation.Group.primary;
            else if (Legislation.Group.secondary.types().contains(legType))
                legGroup = Legislation.Group.secondary;
            else
                return;
            Step4.Mapper.map2(record, context, id, legGroup);
        }

    }

    public static class Mapper2 extends TableMapper<Text, Text> {

        public void map(ImmutableBytesWritable row, Result record, Context context) throws IOException, InterruptedException {
            String id = Bytes.toString(row.get());
            Legislation.Type legType = Legislation.Type.valueOf(id.substring(0, id.indexOf('/')));
            Legislation.Group legGroup;
            if (Legislation.Group.uk.types().contains(legType))
                legGroup = Legislation.Group.uk;
            else if (Legislation.Group.eu.types().contains(legType))
                legGroup = Legislation.Group.eu;
            else
                return;
            Step4.Mapper.map2(record, context, id, legGroup);
        }

    }

    public static class Mapper3 extends TableMapper<Text, Text> {

        public void map(ImmutableBytesWritable row, Result record, Context context) throws IOException, InterruptedException {
            String id = Bytes.toString(row.get());
            Step4.Mapper.map2(record, context, id, Legislation.Group.all);
        }

    }

    static class Reducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] keyParts = key.toString().split("\\|", 4);
            Legislation.Group legGroup = Legislation.Group.valueOf(keyParts[0]);
            short year = Short.parseShort(keyParts[1]);
            Ngrams.Type ngType = Counts.fromShortString(keyParts[2]);
            String ngram = keyParts[3];
            Step4.Reducer.reduce2(context, values, legGroup, year, ngType, ngram);
        }

    }

    public static LinkedHashMap<String, Integer> orderByValue(Map<String, Integer> map) {
        return map.entrySet().stream()
            .sorted((e1, e2) -> Integer.compare(e1.getValue(), e2.getValue()) * -1)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (x, y) -> y, LinkedHashMap::new));
    }

}
