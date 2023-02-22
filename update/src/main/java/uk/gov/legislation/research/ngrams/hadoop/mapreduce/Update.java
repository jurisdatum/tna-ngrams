package uk.gov.legislation.research.ngrams.hadoop.mapreduce;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.Arrays;
import java.util.Date;
import java.util.logging.Logger;

public class Update {

    private static Logger logger = Logger.getLogger(Update.class.getName());

    public static void main(String[] args) throws Exception {
        Integer firstYear;
        try {
            firstYear = args.length == 0 ? null : Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            firstYear = null;
        }
        Integer lastYear;
        try {
            lastYear = args.length < 2 ? null : Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            lastYear = null;
        }
        boolean local = Arrays.stream(args).anyMatch(arg -> arg.equalsIgnoreCase("--local"));
        boolean test = Arrays.stream(args).anyMatch(arg -> arg.equalsIgnoreCase("--test"));
        run(firstYear, lastYear, local, test);
    }

    static final String TestPropertyName = "uk.gov.legislation.update.test";

    public static void run(Integer firstYear, Integer lastYear, boolean local, boolean test) throws Exception {
        Configuration conf = new Configuration();

        Path temp1, temp2;
        if (local) {
            String time = Long.toString(new Date().getTime());
            temp1 = new Path("ngrams-step1-" + time);
            temp2 = new Path("ngrams-step2-" + time);
        } else {
            String time = Long.toString(new Date().getTime());
            temp1 = new Path("hdfs:///ngrams-step1-" + time);
            temp2 = new Path("hdfs:///ngrams-step2-" + time);
        }
        conf.setBoolean(TestPropertyName, test);

        boolean ok;
        ok = Step1.run(conf, firstYear, lastYear, temp1, local);
        if (!ok)
            return;
        ok = Step2.run(conf, temp1, temp2);
        if (!ok)
            return;
        FileSystem.get(conf).delete(temp1, true);
        ok = Step3.run(conf, temp2);
        if (!ok)
            return;
        FileSystem.get(conf).delete(temp2, true);
        ok = Step4.run(conf);
        if (!ok)
            return;
        ok = Step5.run(conf);
        if (!ok)
            return;
        ok = Step6.run(conf);
        if (!ok)
            return;
        Step7.run(conf);
    }

}
