import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.Serializable;

public class Main extends Configured implements Tool {

    public boolean reconstructTrips(String in, String out) throws IOException,
            ClassNotFoundException,
            InterruptedException {

        Job job = Job.getInstance(getConf(), "reconstructingTrips");
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
        FileSystem fs = FileSystem.get(job.getConfiguration());
        if (fs.exists(new Path(out))) {
            fs.delete(new Path(out), true);
        }

        job.setJarByClass(Main.class);

        FileInputFormat.setInputPaths(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.setMapperClass(FormatDataMapper.class);
        job.setReducerClass(ReconstructTripReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TrackInfo.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true);
    }

    public boolean revenue(String in, String out, String revenueType) throws IOException,
            ClassNotFoundException,
            InterruptedException {

        Job job = Job.getInstance(getConf(), "compute revenue");
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
        job.getConfiguration().set("revenue.type", revenueType);
        FileSystem fs = FileSystem.get(job.getConfiguration());
        if (fs.exists(new Path(out))) {
            fs.delete(new Path(out), true);
        }

        job.setJarByClass(Main.class);

        FileInputFormat.setInputPaths(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.setMapperClass(RevenueMapper.class);
        job.setCombinerClass(RevenueReducer.class);
        job.setReducerClass(RevenueReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Main(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        String inputPath = args[0];
        String tripsDir = args[1];
        String revenueDir = args[2];
        String revenueType = args[3];

        // print current configuration in the console
        System.out.println("Input directory: " + inputPath);
        System.out.println("trips_dir: " + tripsDir);
        System.out.println("revenue_dir: " + revenueDir);
        boolean res = reconstructTrips(inputPath, tripsDir);
        if (!res) {
            return -1;
        }
        res = revenue(tripsDir, revenueDir, revenueType);
        if (!res) {
            return -2;
        }
        long endTime = System.currentTimeMillis();
        System.out.printf("elapsed time is %.2f using java.\n", ((endTime - startTime) / 1000.0));
        return 0;
    }
}
