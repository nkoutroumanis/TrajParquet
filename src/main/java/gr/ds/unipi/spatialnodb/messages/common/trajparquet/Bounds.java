package gr.ds.unipi.spatialnodb.messages.common.trajparquet;

import java.io.Serializable;
import java.util.List;

import scala.Tuple3;

public class Bounds implements Serializable {
//    public void setMinLon(double minLon) {
//        this.minLon = minLon;
//    }
//
//    public void setMinLat(double minLat) {
//        this.minLat = minLat;
//    }
//
//    public void setMinTimestamp(long minTs) {
//        this.minTs = minTs;
//    }
//
//    public void setMaxLon(double maxLon) {
//        this.maxLon = maxLon;
//    }
//
//    public void setMaxLat(double maxLat) {
//        this.maxLat = maxLat;
//    }
//
//    public void setMaxTimestamp(long maxTs) {
//        this.maxTs = maxTs;
//    }

    private double minLon = Double.MAX_VALUE;
    private double minLat = Double.MAX_VALUE;
    private long   minTs  = Long.MAX_VALUE;
    private double maxLon = -Double.MAX_VALUE;
    private double maxLat = -Double.MAX_VALUE;
    private long   maxTs  = Long.MIN_VALUE;

    public void add(List<Tuple3<Double, Double, Long>> stPoints) {

//        double minLongitude = Double.MAX_VALUE;
//        double minLatitude = Double.MAX_VALUE;
//        long minTimestamp = Long.MAX_VALUE;
//
//        double maxLongitude = -Double.MAX_VALUE;
//        double maxLatitude = -Double.MAX_VALUE;
//        long maxTimestamp = Long.MIN_VALUE;

        for (Tuple3<Double, Double, Long> stP : stPoints) {
            if (Double.compare(minLon, stP._1()) == 1) {
                minLon = stP._1();
            }
            if (Double.compare(minLat, stP._2()) == 1) {
                minLat = stP._2();
            }
            if (Long.compare(minTs, stP._3()) == 1) {
                minTs =  stP._3();
            }
            if (Double.compare(maxLon, stP._1()) == -1) {
                maxLon = stP._1();
            }
            if (Double.compare(maxLat, stP._2()) == -1) {
                maxLat = stP._2();
            }
            if (Long.compare(maxTs,  stP._3()) == -1) {
                maxTs =  stP._3();
            }
        }
    }

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