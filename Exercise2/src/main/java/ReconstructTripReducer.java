import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ReconstructTripReducer extends Reducer<Text, TrackInfo, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<TrackInfo> values, Context context) throws IOException, InterruptedException {
        List<TrackInfo> valuesList = new ArrayList<>();
        for (TrackInfo info : values) {
            // There are must new Object because info is the same Object in Iterable
            info = new TrackInfo(info.getTaxiID(), info.getStartDate(), info
                    .getStartLatitude(), info.getStartLongitude(), info.isEmpytAtStart(), info.getEndDate(), info
                    .getEndLatitude(), info.getEndLongitude(), info.isEmpytAtEnd());
            valuesList.add(info);
        }
        valuesList.sort(Comparator.comparing(TrackInfo::getStartDate));

        List<TrackInfo> trackList = new ArrayList<>();
        for (TrackInfo info : valuesList) {
            if (info.isEmpytAtStart()) {
                if (isValid(trackList)) {
                    String res = reconstructTrip(trackList);
                    context.write(key, new Text(res));
                }
                trackList.clear();
            } else {
                trackList.add(info);
            }
            if (info.isEmpytAtEnd()) {
                if (isValid(trackList)) {
                    String res = reconstructTrip(trackList);
                    context.write(key, new Text(res));
                }
                trackList.clear();
            } else if (info.isEmpytAtStart()) {
                trackList.add(info);
            }
        }
    }

    private String reconstructTrip(List<TrackInfo> trackInfoList) {
        float startLatitude;
        float startLongitude;
        float endLatitude;
        float endLongitude;
        String startDate;
        String endDate;
        // get start position
        TrackInfo firstInfo = trackInfoList.get(0);
        if (!firstInfo.isEmpytAtStart()) {
            startLatitude = firstInfo.getStartLatitude();
            startLongitude = firstInfo.getStartLongitude();
            startDate = firstInfo.getStartDate();
        } else {
            startLatitude = firstInfo.getEndLatitude();
            startLongitude = firstInfo.getEndLongitude();
            startDate = firstInfo.getEndDate();
        }
        // get end position
        TrackInfo lastInfo = trackInfoList.get(trackInfoList.size() - 1);
        if (!lastInfo.isEmpytAtEnd()) {
            endLatitude = lastInfo.getEndLatitude();
            endLongitude = lastInfo.getEndLongitude();
            endDate = lastInfo.getEndDate();
        } else {
            endLatitude = lastInfo.getStartLatitude();
            endLongitude = lastInfo.getStartLongitude();
            endDate = lastInfo.getStartDate();
        }

        // get month
        String month = startDate.substring(0, 7);

        // get trip distance
        double tripDistance = TrackInfo.getDis(startLatitude, startLongitude, endLatitude,
                endLongitude);

        double tripRevenue = 3.5 + tripDistance * 1.71;
        return String.format("%s,%f,%s,%f,%f,%s,%f,%f", month, tripRevenue, startDate,
                startLatitude, startLongitude, endDate, endLatitude, endLongitude);
    }

    public static boolean isValid(List<TrackInfo> trackInfoList) {
        if (trackInfoList.size() <= 1) {
            return false;
        }
        boolean passAirport = false;
        for (TrackInfo info : trackInfoList) {
            if (!info.isValid()) {
                return false;
            }
            if (info.isWithinAirport1km()) {
                passAirport = true;
            }
        }
        return passAirport;
    }
}
