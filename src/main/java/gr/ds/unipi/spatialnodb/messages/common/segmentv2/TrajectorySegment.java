package gr.ds.unipi.spatialnodb.messages.common.segmentv2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class TrajectorySegment implements Serializable {

    private final String objectId;

    private final long segment;

    private final SpatioTemporalPoints spatioTemporalPoints;

    private final double minLongitude;
    private final double minLatitude;
    private final long minTimestamp;
    private final double maxLongitude;
    private final double maxLatitude;
    private final long maxTimestamp;

//    public TrajectorySegment(String objectId, long trajectoryId, long segment, SpatioTemporalPoints spatioTemporalPoints, double minLongitude, double minLatitude, long minTimestamp, double maxLongitude, double maxLatitude, long maxTimestamp) {
//        this.objectId = objectId;
//        this.trajectoryId = trajectoryId;
//        this.segment = segment;
//        this.spatioTemporalPoints = spatioTemporalPoints;
//        this.minLongitude = minLongitude;
//        this.minLatitude = minLatitude;
//        this.minTimestamp = minTimestamp;
//        this.maxLongitude = maxLongitude;
//        this.maxLatitude = maxLatitude;
//        this.maxTimestamp = maxTimestamp;
//    }

    public TrajectorySegment(String objectId, long segment, SpatioTemporalPoints spatioTemporalPoints, double minLongitude, double minLatitude, long minTimestamp, double maxLongitude, double maxLatitude, long maxTimestamp) {
        this.objectId = objectId;
        this.segment = segment;
        this.spatioTemporalPoints = spatioTemporalPoints;
        this.minLongitude = minLongitude;
        this.minLatitude = minLatitude;
        this.minTimestamp = minTimestamp;
        this.maxLongitude = maxLongitude;
        this.maxLatitude = maxLatitude;
        this.maxTimestamp = maxTimestamp;
    }

    public TrajectorySegment(String objectId, long segment, List<TrajectorySegment> trajectorySegments) {

        this.objectId = objectId;
//        String objectId = trajectorySegments.get(0).getObjectId();
//
//        for (int i = 1; i < trajectorySegments.size(); i++) {
//            if(!objectId.equals(trajectorySegments.get(i).getObjectId())){
//                try {
//                    throw new Exception("The concatenation of continuous segments is erroneous due to objectId");
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        }

//        this.trajectoryId=-1;
        this.segment = segment;

//        int spatioTemporalPointsNum = trajectorySegments.get(0).getSpatioTemporalPoints().getSptCount();
//
//        //SpatioTemporalPoint spatioTemporalPoint = trajectorySegments.get(0).getSpatioTemporalPoints()[trajectorySegments.get(0).getSpatioTemporalPoints().length-1];
//
//        for (int i = 1; i < trajectorySegments.size(); i++) {
//            spatioTemporalPointsNum = spatioTemporalPointsNum + (trajectorySegments.get(i).getSpatioTemporalPoints().getSptCount()-1);
////            if(!spatioTemporalPoint.equals(trajectorySegments.get(i).spatioTemporalPoints[0])){
////                try {
////                    throw new Exception("The concatenation of continuous segments is erroneous due to non continuous trajectory segments");
////                } catch (Exception e) {
////                    e.printStackTrace();
////                }
////            }
////            spatioTemporalPoint = trajectorySegments.get(i).getSpatioTemporalPoints()[trajectorySegments.get(i).getSpatioTemporalPoints().length-1];
//        }

        SpatioTemporalPoints.Builder sptsBuilder = SpatioTemporalPoints.newBuilder();

//        int arrayIndex = 0;

        for (SpatioTemporalPoints.SpatioTemporalPoint spatioTemporalPoint : trajectorySegments.get(0).spatioTemporalPoints.getSptList()) {
            sptsBuilder.addSpt(spatioTemporalPoint);
        }

        for (int i = 1; i < trajectorySegments.size(); i++) {
            for (int j = 1; j < trajectorySegments.get(i).spatioTemporalPoints.getSptList().size(); j++) {
                sptsBuilder.addSpt(trajectorySegments.get(i).spatioTemporalPoints.getSpt(j));
            }
        }

        this.spatioTemporalPoints = sptsBuilder.build();

        this.minLongitude = -1;
        this.minLatitude = -1;
        this.minTimestamp = -1;
        this.maxLongitude = -1;
        this.maxLatitude = -1;
        this.maxTimestamp = -1;
    }

    public String getObjectId() {
        return objectId;
    }

//    public long getTrajectoryId() {
//        return trajectoryId;
//    }

    public long getSegment() {
        return segment;
    }

    public double getMinLongitude() {
        return minLongitude;
    }

    public double getMinLatitude() {
        return minLatitude;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public double getMaxLongitude() {
        return maxLongitude;
    }

    public double getMaxLatitude() {
        return maxLatitude;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public SpatioTemporalPoints getSpatioTemporalPoints() {
        return spatioTemporalPoints;
    }

    @Override
    public String toString() {
        return "TrajectorySegment{" +
                "objectId='" + objectId + '\'' +
//                ", trajectoryId=" + trajectoryId +
                ", segment=" + segment +
                ", spatioTemporalPoints=" + spatioTemporalPoints.toString() +
                ", minLongitude=" + minLongitude +
                ", minLatitude=" + minLatitude +
                ", minTimestamp=" + minTimestamp +
                ", maxLongitude=" + maxLongitude +
                ", maxLatitude=" + maxLatitude +
                ", maxTimestamp=" + maxTimestamp +
                '}';
    }
}
