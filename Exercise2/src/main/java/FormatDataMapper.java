import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FormatDataMapper extends Mapper<LongWritable, Text, Text, TrackInfo> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * value format:
         * <taxi-id>, <start date>, <start pos (lat)>, <start pos (long)>, <start status> . . .
         * . . . <end date> <end pos (lat)> <end pos (long)> <end status>
         */
        String line = value.toString();
        TrackInfo trackInfo = parse(line);
        if (trackInfo != null) {
            context.write(new Text(trackInfo.getTaxiID()), trackInfo);
        }
    }


    public static TrackInfo parse(String line) {
        String[] info = line.split(",");
        for (String s : info) {
            if (s.equals("NULL")) {
                return null;
            }
        }
        String taxiID = info[0];

        String startDate = info[1].substring(1, info[1].length() - 1);


        float startLatitude = Float.parseFloat(info[2]);
        float startLongitude = Float.parseFloat(info[3]);
        boolean isEmpytAtStart = info[4].equals("'E'");

        String endDate = info[5].substring(1, info[1].length() - 1);
        float endLatitude = Float.parseFloat(info[6]);
        float endLongitude = Float.parseFloat(info[7]);
        boolean isEmpytAtEnd = info[8].equals("'E'");

        TrackInfo trackInfo = new TrackInfo(taxiID, startDate, startLatitude, startLongitude,
                isEmpytAtStart, endDate, endLatitude, endLongitude, isEmpytAtEnd);
        return trackInfo;
    }
}

