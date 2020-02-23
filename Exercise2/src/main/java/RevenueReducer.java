import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class RevenueReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
            InterruptedException {
        float total = 0;
        for (Text revenue : values) {
            total += Float.parseFloat(revenue.toString());
        }
        context.write(key, new Text(String.valueOf(total)));
    }
}
