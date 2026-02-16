package gr.ds.unipi.spatialnodb.messages.common.trajparquet;

import java.io.Serializable;

public class Bounds implements Serializable {
    private double minLon = Double.MAX_VALUE;
    private double minLat = Double.MAX_VALUE;
    private long   minTs  = Long.MAX_VALUE;
    private double maxLon = -Double.MAX_VALUE;
    private double maxLat = -Double.MAX_VALUE;
    private long   maxTs  = Long.MIN_VALUE;

    public void add(TrajectorySegment ts) {
        minLon = Math.min(minLon, ts.getMinLongitude());
        minLat = Math.min(minLat, ts.getMinLatitude());
        minTs  = Math.min(minTs,  ts.getMinTimestamp());
        maxLon = Math.max(maxLon, ts.getMaxLongitude());
        maxLat = Math.max(maxLat, ts.getMaxLatitude());
        maxTs  = Math.max(maxTs,  ts.getMaxTimestamp());
    }

    public void merge(Bounds o) {
        minLon = Math.min(minLon, o.minLon);
        minLat = Math.min(minLat, o.minLat);
        minTs  = Math.min(minTs,  o.minTs);
        maxLon = Math.max(maxLon, o.maxLon);
        maxLat = Math.max(maxLat, o.maxLat);
        maxTs  = Math.max(maxTs,  o.maxTs);
    }

    public double getMinLongitude() {
        return minLon;
    }
    public double getMinLatitude() {
        return minLat;
    }
    public long getMinTimestamp() {
        return minTs;
    }
    public double getMaxLongitude() {
        return maxLon;
    }
    public double getMaxLatitude() {
        return maxLat;
    }
    public long getMaxTimestamp() {
        return maxTs;
    }


}