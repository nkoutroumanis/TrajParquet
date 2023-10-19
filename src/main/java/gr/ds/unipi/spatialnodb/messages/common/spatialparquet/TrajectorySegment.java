package gr.ds.unipi.spatialnodb.messages.common.spatialparquet;

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

        int spatioTemporalPointsNum = trajectorySegments.get(0).getSpatioTemporalPoints().length;

        //SpatioTemporalPoint spatioTemporalPoint = trajectorySegments.get(0).getSpatioTemporalPoints()[trajectorySegments.get(0).getSpatioTemporalPoints().length-1];

        for (int i = 1; i < trajectorySegments.size(); i++) {
            spatioTemporalPointsNum = spatioTemporalPointsNum + (trajectorySegments.get(i).getSpatioTemporalPoints().length-1);
//            if(!spatioTemporalPoint.equals(trajectorySegments.get(i).spatioTemporalPoints[0])){
//                try {
//                    throw new Exception("The concatenation of continuous segments is erroneous due to non continuous trajectory segments");
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//            spatioTemporalPoint = trajectorySegments.get(i).getSpatioTemporalPoints()[trajectorySegments.get(i).getSpatioTemporalPoints().length-1];
        }

        this.spatioTemporalPoints = new SpatioTemporalPoint[spatioTemporalPointsNum];
        int arrayIndex = 0;
        for (SpatioTemporalPoint spPoint : trajectorySegments.get(0).spatioTemporalPoints) {
            spatioTemporalPoints[arrayIndex++] = spPoint;
        }

        for (int i = 1; i < trajectorySegments.size(); i++) {
            for (int j = 1; j < trajectorySegments.get(i).getSpatioTemporalPoints().length; j++) {
                spatioTemporalPoints[arrayIndex++] = trajectorySegments.get(i).getSpatioTemporalPoints()[j];
            }
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
