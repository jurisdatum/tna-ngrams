package uk.gov.legislation.research.ngrams.hadoop.mapreduce;

import uk.gov.legislation.research.Clml;
import uk.gov.legislation.research.Legislation;
import uk.gov.legislation.research.TextExtractor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import uk.gov.legislation.research.ngrams.hadoop.hbase.Documents;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Step3 {

    private static Logger logger = Logger.getLogger(Step3.class.getName());

    public static boolean run(Configuration conf, Path input) throws IOException, InterruptedException, ClassNotFoundException {
        logger.info("beginning step 3");
        Documents.ensureTableExists(conf);
        Job job = Job.getInstance(conf);
        job.setJobName("step 3: write documents to HBase");
        job.setJarByClass(Step3.class);
        FileInputFormat.addInputPath(job, input);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(Mapper.class);
        job.setNumReduceTasks(0);
//        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
//        job.setMapOutputValueClass(Put.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, Documents.tableName.getNameAsString());
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        return job.waitForCompletion(true);
    }

    public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<Text, Text, ImmutableBytesWritable, Put> {

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = key.toString().split("/", 2);
            final Legislation.Type legType = Legislation.Type.valueOf(parts[0]);
            final short year = Short.parseShort(parts[1]);
            final String id = value.toString();

            Thread.sleep(250);

            String firstVersion;
            if (Legislation.Group.primary.types().contains(legType))
                firstVersion = "enacted";
            else if (Legislation.Group.secondary.types().contains(legType))
                firstVersion = "made";
            else if (Legislation.Group.eu.types().contains(legType))
                firstVersion = "adopted";
            else
                throw new IOException("unknown legislation group for " + legType.name());
            byte[] rawClml;
            try {
                rawClml = Clml.getRaw(id, firstVersion);
            } catch (Clml.NoDocumentException e) {
                logger.warning(e.responseCode + " error reading " + id);
                Put put = Documents.put(id, year, null, null, (short) e.responseCode, null);
                context.write(new ImmutableBytesWritable(put.getRow()), put);
                return;
            } catch (Clml.NoClmlException e) {
                logger.info("no CLML for " + id);
                Put put = Documents.put(id, year, null, null, (short) 204, null);
                context.write(new ImmutableBytesWritable(put.getRow()), put);
                return;
            } catch (Exception e) {
                logger.log(Level.WARNING, "error reading " + id, e);
                Put put = Documents.put(id, year, null, null, (short) 500, null);
                context.write(new ImmutableBytesWritable(put.getRow()), put);
                return;
            }
            Clml parsed = new Clml(new ByteArrayInputStream(rawClml));
            if (!parsed.hasBody()) {
                logger.info(id + " has no body");
                Put put = Documents.put(id, year, null, null, (short) 204, null);
                context.write(new ImmutableBytesWritable(put.getRow()), put);
                return;
            }
            logger.info("mapping: " + id);
            String text = new TextExtractor().extract(parsed);
            Date date = parsed.getDate();
            String title = parsed.getTitle();
            Put put = Documents.put(id, year, date, title, (short) 200, text);
            context.write(new ImmutableBytesWritable(put.getRow()), put);
        }

    }
}
