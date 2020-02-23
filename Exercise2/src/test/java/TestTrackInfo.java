import junit.framework.TestSuite;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.text.ParseException;
import java.util.Scanner;

public class TestTrackInfo extends TestSuite {

    private void printSpreedInfo(String line) throws ParseException {
        TrackInfo trackInfo = FormatDataMapper.parse(line);
        System.out.println(trackInfo.getDistance());
        System.out.println(trackInfo.getTime(trackInfo.getStartDate(), trackInfo.getEndDate()) /
                1000 / 60.0 / 60.0);
        System.out.println(trackInfo.getSpeed());
    }

    @Test
    public void testGetSpeed() throws ParseException {
        String line = "576,'2010-03-01 00:12:20',37.77227,-122.49204,'M','2010-03-01 00:13:40'," +
                "37.77189,-122.50046,'M'";
        printSpreedInfo(line);
        line = "1055,'2010-02-28 22:33:12',37.78145,0.55,'M','2010-03-01 01:03:08',37.77286," +
                "-122.43735,'E'";
        printSpreedInfo(line);
    }

    @Test
    public void testGetDis() throws ParseException {
        double res = TrackInfo.getDis(37.6158f, -122.3893f, 37.79527f, -122.41146f);
        Assert.assertEquals(20.05121336003112, res, 0.01);
    }

    public void tesetParse() throws FileNotFoundException, ParseException {
        Scanner in = new Scanner(new FileInputStream("../data/2010_03.segments"));
        int cnt = 0;
        while (in.hasNext()) {
            cnt += 1;
            if (cnt % 1000000 == 0) {
                System.out.println(cnt);
            }
            String line = in.nextLine();
            TrackInfo trackInfo = FormatDataMapper.parse(line);
            if (trackInfo.isValid() && trackInfo.isWithinAirport1km()) {
                System.out.println(line);
                break;
            }
        }
    }

}
