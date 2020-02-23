import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class TrackInfo implements Writable {

    private double distance;
    private String taxiID;
    private String startDate;
    private float startLatitude;
    private float startLongitude;
    private boolean isEmpytAtStart;
    private String endDate;
    private float endLatitude;
    private float endLongitude;
    private boolean isEmpytAtEnd;
    private boolean isValid;

    public TrackInfo() {

    }

    public TrackInfo(String taxiID, String startDate, float startLatitude, float startLongitude,
                     boolean isEmpytAtStart, String endDate, float endLatitude, float endLongitude, boolean isEmpytAtEnd) {
        this.taxiID = taxiID;
        this.startDate = startDate;
        this.startLatitude = startLatitude;
        this.startLongitude = startLongitude;
        this.isEmpytAtStart = isEmpytAtStart;
        this.endDate = endDate;
        this.endLatitude = endLatitude;
        this.endLongitude = endLongitude;
        this.isEmpytAtEnd = isEmpytAtEnd;
        this.isValid = true;
        this.distance = getDis(startLatitude, startLongitude, endLatitude, endLongitude);
        try {
            if (getTime(startDate, endDate) > 1800 * 1000) {
                isValid = false;
            }
        } catch (ParseException e) {
            e.printStackTrace();
            isValid = false;
        }
        try {
            if (getSpeed() > 200) {
                isValid = false;
            }
        } catch (ParseException e) {
            isValid = false;
            e.printStackTrace();
        }
    }

    /**
     * get distance between two position
     *
     * @param startLatitude
     * @param startLongitude
     * @param endLatitude
     * @param endLongitude
     * @return
     */
    public static double getDis(double startLatitude, double startLongitude, double endLatitude,
                                double endLongitude) {
        double deltaLatitude = (startLatitude - endLatitude) * 2 * Math.PI / 360;
        double deltaLongitude = (startLongitude - endLongitude) * 2 * Math.PI / 360;
        double meanLatitude = (startLatitude + endLatitude) / 2.0 * 2 * Math.PI / 360;
        double R = 6371.009;
        return (R * Math.sqrt(Math.pow(deltaLatitude, 2) + Math.pow((Math.cos(meanLatitude) * deltaLongitude), 2)));
    }

    /**
     * Calculate time interval
     *
     * @return the number of milliseconds
     */
    public static long getTime(String startDate, String endDate) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long startTime = simpleDateFormat.parse(startDate).getTime();
        long endTime = simpleDateFormat.parse(endDate).getTime();
        return Math.abs(endTime - startTime);
    }

    public double getSpeed() throws ParseException {
        float costTime = (float) (getTime(startDate, endDate));
        costTime = (float) (costTime / 1000.0 / 60 / 60);
        return this.distance / costTime;
    }

    /**
     * Is it within 1km of the airport
     *
     * @return
     */
    public boolean isWithinAirport1km() {
        double disStartToAir = getDis(startLatitude, startLongitude, 37.62131f, -122.37896f);
        double disEndToAir = getDis(endLatitude, endLongitude, 37.62131f, -122.37896f);

        return disStartToAir < 1.0 || disEndToAir < 1.0;
    }

    public String getStartDate() {
        return startDate;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public float getStartLatitude() {
        return startLatitude;
    }

    public void setStartLatitude(float startLatitude) {
        this.startLatitude = startLatitude;
    }

    public float getStartLongitude() {
        return startLongitude;
    }

    public void setStartLongitude(float startLongitude) {
        this.startLongitude = startLongitude;
    }

    public boolean isEmpytAtStart() {
        return isEmpytAtStart;
    }

    public void setEmpytAtStart(boolean empytAtStart) {
        isEmpytAtStart = empytAtStart;
    }

    public String getEndDate() {
        return endDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }

    public float getEndLatitude() {
        return endLatitude;
    }

    public void setEndLatitude(float endLatitude) {
        this.endLatitude = endLatitude;
    }

    public float getEndLongitude() {
        return endLongitude;
    }

    public void setEndLongitude(float endLongitude) {
        this.endLongitude = endLongitude;
    }

    public boolean isEmpytAtEnd() {
        return isEmpytAtEnd;
    }

    public void setEmpytAtEnd(boolean empytAtEnd) {
        isEmpytAtEnd = empytAtEnd;
    }

    @Override
    public String toString() {
        try {
            return "TrackInfo{" +
                    "startDate='" + startDate + '\'' +
                    ", startLatitude=" + startLatitude +
                    ", startLongitude=" + startLongitude +
                    ", isEmpytAtStart=" + isEmpytAtStart +
                    ", endDate='" + endDate + '\'' +
                    ", endLatitude=" + endLatitude +
                    ", endLongitude=" + endLongitude +
                    ", isEmpytAtEnd=" + isEmpytAtEnd +
                    ", speed=" + getSpeed() +
                    ", isWithinAirport1km=" + isWithinAirport1km() +
                    '}';
        } catch (ParseException e) {
            e.printStackTrace();
            return "";
        }
    }

    public String getTaxiID() {
        return taxiID;
    }

    public void setTaxiID(String taxiID) {
        this.taxiID = taxiID;
    }

    public boolean isValid() {
        return isValid;
    }

    public void setValid(boolean valid) {
        isValid = valid;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    public double getDistance() {
        return distance;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(distance);
        dataOutput.writeBytes(taxiID + "\n");
        dataOutput.writeBytes(startDate + "\n");
        dataOutput.writeFloat(startLatitude);
        dataOutput.writeFloat(startLongitude);
        dataOutput.writeBoolean(isEmpytAtStart);
        dataOutput.writeBytes(endDate + "\n");
        dataOutput.writeFloat(endLatitude);
        dataOutput.writeFloat(endLongitude);
        dataOutput.writeBoolean(isEmpytAtEnd);
        dataOutput.writeBoolean(isValid);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.distance = dataInput.readDouble();
        this.taxiID = dataInput.readLine();
        this.startDate = dataInput.readLine();
        this.startLatitude = dataInput.readFloat();
        this.startLongitude = dataInput.readFloat();
        this.isEmpytAtStart = dataInput.readBoolean();
        this.endDate = dataInput.readLine();
        this.endLatitude = dataInput.readFloat();
        this.endLongitude = dataInput.readFloat();
        this.isEmpytAtEnd = dataInput.readBoolean();
        this.isValid = dataInput.readBoolean();
    }

}
