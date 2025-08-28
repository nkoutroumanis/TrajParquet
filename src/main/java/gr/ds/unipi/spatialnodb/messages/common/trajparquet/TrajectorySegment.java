package gr.ds.unipi.spatialnodb.messages.common.trajparquet;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class TrajectorySegment implements Serializable {

    private final String objectId;

    private final long segment;

    private final SpatioTemporalPoint[] spatioTemporalPoints;

    private final double minLongitude;
    private final double minLatitude;
    private final long minTimestamp;
    private final double maxLongitude;
    private final double maxLatitude;
    private final long maxTimestamp;

//    public TrajectorySegment(String objectId, long trajectoryId, long segment, SpatioTemporalPoint[] spatioTemporalPoints, double minLongitude, double minLatitude, long minTimestamp, double maxLongitude, double maxLatitude, long maxTimestamp) {
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

    public TrajectorySegment(String objectId, long segment, SpatioTemporalPoint[] spatioTemporalPoints, double minLongitude, double minLatitude, long minTimestamp, double maxLongitude, double maxLatitude, long maxTimestamp) {
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

        if(trajectorySegments.size()!=1) {


            this.objectId = objectId;

//        this.trajectoryId=-1;
            this.segment = segment;

            int spatioTemporalPointsNum = trajectorySegments.get(0).getSpatioTemporalPoints().length - 1;
            spatioTemporalPointsNum = spatioTemporalPointsNum + trajectorySegments.get(trajectorySegments.size() - 1).getSpatioTemporalPoints().length - 1;

            for (int i = 1; i < trajectorySegments.size() - 1; i++) {
                spatioTemporalPointsNum = spatioTemporalPointsNum + (trajectorySegments.get(i).getSpatioTemporalPoints().length - 2);
            }

            this.spatioTemporalPoints = new SpatioTemporalPoint[spatioTemporalPointsNum];
            int arrayIndex = 0;

            for (int i = 0; i < trajectorySegments.get(0).spatioTemporalPoints.length - 1; i++) {
                spatioTemporalPoints[arrayIndex++] = trajectorySegments.get(0).spatioTemporalPoints[i];
            }

            for (int i = 1; i < trajectorySegments.size() - 1; i++) {
                for (int j = 1; j < trajectorySegments.get(i).getSpatioTemporalPoints().length - 1; j++) {
                    spatioTemporalPoints[arrayIndex++] = trajectorySegments.get(i).getSpatioTemporalPoints()[j];
                }
            }

            for (int i = 1; i < trajectorySegments.get(trajectorySegments.size() - 1).spatioTemporalPoints.length; i++) {
                spatioTemporalPoints[arrayIndex++] = trajectorySegments.get(trajectorySegments.size() - 1).spatioTemporalPoints[i];
            }
        }
        else{

            this.objectId = objectId;
            this.segment = segment;
            this.spatioTemporalPoints = trajectorySegments.get(0).getSpatioTemporalPoints();

//            for (SpatioTemporalPoint spatioTemporalPoint : spatioTemporalPoints) {
//                if(spatioTemporalPoint==null){
//                    try {
//                        throw new Exception("NULL");
//                    } catch (Exception e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//            }

        }

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

    public SpatioTemporalPoint[] getSpatioTemporalPoints() {
        return spatioTemporalPoints;
    }

    @Override
    public String toString() {
        return "TrajectorySegment{" +
                "objectId='" + objectId + '\'' +
//                ", trajectoryId=" + trajectoryId +
                ", segment=" + segment +
                ", spatioTemporalPoints=" + Arrays.toString(spatioTemporalPoints) +
                ", minLongitude=" + minLongitude +
                ", minLatitude=" + minLatitude +
                ", minTimestamp=" + minTimestamp +
                ", maxLongitude=" + maxLongitude +
                ", maxLatitude=" + maxLatitude +
                ", maxTimestamp=" + maxTimestamp +
                '}';
    }
}
