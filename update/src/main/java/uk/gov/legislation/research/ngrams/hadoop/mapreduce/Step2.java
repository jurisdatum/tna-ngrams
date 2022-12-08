package uk.gov.legislation.research.ngrams.hadoop.mapreduce;

import uk.gov.legislation.research.Atom;
import uk.gov.legislation.research.Legislation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.Set;
import java.util.logging.Logger;

class Step2 {

    private static Logger logger = Logger.getLogger(Step2.class.getName());

    static boolean run(Configuration conf, Path input, Path output) throws Exception {
        logger.info("beginning step 2");
        Job job = Job.getInstance(conf);
        job.setJobName("step 2: scrape LGU for doc ids");
        job.setJarByClass(Step2.class);
        FileInputFormat.setInputPaths(job, input);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(Mapper.class);
        job.setNumReduceTasks(0);
        FileOutputFormat.setOutputPath(job, output);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true);
    }

    private static class Mapper extends org.apache.hadoop.mapreduce.Mapper<Text, IntWritable, Text, Text> {

        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            Legislation.Type docType = Legislation.Type.valueOf(key.toString());
            int year = value.get();
            logger.info("scraping doc ids for " + key.toString() + "/" + year);
            Thread.sleep(1000);
            Set<String> docIds;
            try {
                docIds = Atom.getIdsForYear(docType, year);
            } catch (Exception e) {
                return;
            }
            for (String docId : docIds) {
                logger.fine("mapping " + key.toString() + "/" + year + " -> " + docId);
                context.write(new Text(key.toString() + "/" + year), new Text(docId));
            }
        }

    }

}
