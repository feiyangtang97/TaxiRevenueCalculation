import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RevenueMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * value format:
         * <taxi-id>, <month>, <revenue>, <start date>, <start pos (lat)>, <start pos (long)>,
         * <end date>, <end pos (lat)>, <end pos (long)>
         */
        String line = value.toString();
        String[] info = line.split(",");
        if (context.getConfiguration().get("revenue.type").equals("month")) {
            context.write(new Text(info[1]), new Text(info[2]));
        } else {
            context.write(new Text(info[3].substring(0, 10)), new Text(info[2]));
        }
    }
}

