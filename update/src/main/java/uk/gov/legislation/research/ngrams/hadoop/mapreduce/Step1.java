package uk.gov.legislation.research.ngrams.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.logging.Logger;

class Step1 {

    private static Logger logger = Logger.getLogger(Step1.class.getName());

    private static final String cutoffParameter1 = "step1.cutoff1";
    private static final String cutoffParameter2 = "step1.cutoff2";

    private static final String localPathToDoctypes = "/uk/gov/legislation/research/ngrams/hadoop/mapreduce/doctypes.tsv";

    private static Path makeHDFSPathForDoctypes(boolean local) {
        String raw = "doctypes.tsv";
        if (local)
            return new Path(raw);
        return new Path("hdfs:///" + raw);
    }

    private static Path copyDoctypesToHDFS(Configuration conf, boolean local) throws IOException {
        InputStream input = Step1.class.getResourceAsStream(localPathToDoctypes);
        FileSystem fileSystem = FileSystem.get(conf);
        Path outPath = makeHDFSPathForDoctypes(local);
        FSDataOutputStream output = fileSystem.create(outPath);
        byte[] b = new byte[1024];
        int numBytes = 0;
        while ((numBytes = input.read(b)) > 0)
            output.write(b, 0, numBytes);
        input.close();
        output.close();
        fileSystem.close();
        return outPath;
    }

    static boolean run(Configuration conf, Integer firstYear, Integer lastYear, Path output, boolean local) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        if (firstYear != null)
            conf.setInt(cutoffParameter1, firstYear.intValue());
        if (lastYear != null)
            conf.setInt(cutoffParameter2, lastYear.intValue());
        Job job = Job.getInstance(conf, "step 1: enumerate years for doc types");
        job.setJarByClass(Step1.class);
        job.setMapperClass(Mapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path input = copyDoctypesToHDFS(conf, local);

        FileInputFormat.addInputPath(job, input);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileOutputFormat.setOutputPath(job, output);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        return job.waitForCompletion(true);
    }

    private static class Mapper extends org.apache.hadoop.mapreduce.Mapper<Text, Text, Text, IntWritable> {

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            boolean test = context.getConfiguration().getBoolean(Update.TestPropertyName, false);
            if (test && !key.toString().equals("ukpga"))
                return;
            int cutoff1 = context.getConfiguration().getInt(cutoffParameter1, 1900);
            int cutoff2 = context.getConfiguration().getInt(cutoffParameter2, Calendar.getInstance().get(Calendar.YEAR));
            if (value.toString().equals("0"))
                return;
            String[] parts = value.toString().split("-", 2);
            int firstYear = Integer.parseInt(parts[0]);
            int lastYear = parts[1].equals("") ?  cutoff2 : Integer.parseInt(parts[1]);
            if (firstYear < cutoff1)
                firstYear = cutoff1;
            if (lastYear > cutoff2)
                lastYear = cutoff2;
            if (lastYear < firstYear)
                return;
            for (int year = firstYear; year <= lastYear; year++) {
                logger.fine("mapping " + key.toString() + " -> " + year);
                context.write(key, new IntWritable(year));
            }
        }

    }

}
