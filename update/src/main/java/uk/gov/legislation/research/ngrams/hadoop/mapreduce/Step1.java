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

    static final String cutoffParameter = "step1.cutoff";

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

    static boolean run(Configuration conf, Integer cuttoff, Path output, boolean local) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        if (cuttoff != null)
            conf.setInt(cutoffParameter, cuttoff.intValue());
        Job job = Job.getInstance(conf, "step 1: enumerate years for doc types");
        job.setJarByClass(Step1.class);
        job.setMapperClass(Mapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        Path input;
//        if (local)
//            input = new Path(Step1.class.getResource("/uk/gov/legislation/research/ngrams/hadoop/mapreduce/doctypes.tsv").toURI());
//        else
//            input = new Path("s3://misc.tna.jurisdatum.com/doctypes.tsv");
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
            int cutoff = context.getConfiguration().getInt(cutoffParameter, 1900);
            if (value.toString().equals("0"))
                return;
            String[] parts = value.toString().split("-", 2);
            int firstYear, lastYear;
            firstYear = Integer.parseInt(parts[0]);
            if (parts[1].equals(""))
                lastYear = Calendar.getInstance().get(Calendar.YEAR);
            else
                lastYear = Integer.parseInt(parts[1]);
            if (firstYear < cutoff)
                firstYear = cutoff;
            for (int year = firstYear; year <= lastYear; year++) {
                logger.fine("mapping " + key.toString() + " -> " + year);
                context.write(key, new IntWritable(year));
            }
        }

    }

}
