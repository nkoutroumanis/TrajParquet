package gr.ds.unipi.spatialnodb.messages.common.segmentv7;

import java.io.Serializable;

public class SpatioTemporalPoint implements Serializable {

    public double getLongitude() {
        return longitude;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SpatioTemporalPoint that = (SpatioTemporalPoint) o;

        if (Double.compare(that.getLongitude(), getLongitude()) != 0) return false;
        if (Double.compare(that.getLatitude(), getLatitude()) != 0) return false;
        return getTimestamp() == that.getTimestamp();
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(getLongitude());
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(getLatitude());
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (int) (getTimestamp() ^ (getTimestamp() >>> 32));
        return result;
    }

    public double getLatitude() {
        return latitude;
    }

    public long getTimestamp() {
        return timestamp;
    }

    private final double longitude;
    private final double latitude;
    private final long timestamp;


    public SpatioTemporalPoint(double longitude, double latitude, long timestamp) {
        this.longitude = longitude;
        this.latitude = latitude;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "SpatioTemporalPoint{" +
                "longitude=" + longitude +
                ", latitude=" + latitude +
                ", timestamp=" + timestamp +
                '}';
    }
}
