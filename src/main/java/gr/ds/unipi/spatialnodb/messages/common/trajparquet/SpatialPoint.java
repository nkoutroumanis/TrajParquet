package gr.ds.unipi.spatialnodb.messages.common.trajparquet;

import java.io.Serializable;
import java.util.Objects;

public class SpatialPoint implements Serializable {

    public double getLongitude() {
        return longitude;
    }

    public double getLatitude() {
        return latitude;
    }


    private final double longitude;
    private final double latitude;


    public SpatialPoint(double longitude, double latitude) {
        this.longitude = longitude;
        this.latitude = latitude;
    }

    @Override
    public String toString() {
        return "SpatioTemporalPoint{" +
                "longitude=" + longitude +
                ", latitude=" + latitude +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SpatialPoint)) return false;
        SpatialPoint that = (SpatialPoint) o;
        return Double.compare(longitude, that.longitude) == 0 && Double.compare(latitude, that.latitude) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(longitude, latitude);
    }
}
