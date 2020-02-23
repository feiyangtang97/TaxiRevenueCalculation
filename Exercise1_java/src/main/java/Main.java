import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;

public class Main implements Serializable {

    static class MyComaprator implements Comparator<Double>, Serializable {

        @Override
        public int compare(Double o1, Double o2) {
            return Double.compare(o1, o2);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName
                ("trip_distance").setMaster("local[2]"));
        long startTime = System.currentTimeMillis();
        Configuration conf = new Configuration(sc.hadoopConfiguration());
        String input_path = args[0];

        // print current configuration in the console
        System.out.println("Input directory: " + input_path);

        JavaRDD<String> textLines = sc.newAPIHadoopFile(input_path, TextInputFormat.class,
                LongWritable.class, Text.class, conf).map(kv -> kv._2.toString());

        JavaRDD<Double> distanceLines = textLines.map((Function<String, Double>) s -> {
            String[] info = s.split("\\s");
            float start_latitude = Float.parseFloat(info[2]);
            float start_longitude = Float.parseFloat(info[3]);
            float end_latitude = Float.parseFloat(info[5]);
            float end_longitude = Float.parseFloat(info[6]);
            double delta_latitude = (start_latitude - end_latitude) * 2 * Math.PI / 360;
            double delta_longitude = (start_longitude - end_longitude) * 2 * Math.PI / 360;
            double mean_latitude = (start_latitude + end_latitude) / 2.0 * 2 * Math.PI / 360;
            double R = 6371.009;
            return R * Math.sqrt(Math.pow(delta_latitude, 2) + Math.pow((Math.cos(mean_latitude) * delta_longitude), 2));
        });
        System.out.println(distanceLines.max(new MyComaprator()));
        long endTime = System.currentTimeMillis();
        System.out.printf("elapsed time is %.2f using java.\n", ((endTime - startTime) / 1000.0));
        sc.close();
    }

}
