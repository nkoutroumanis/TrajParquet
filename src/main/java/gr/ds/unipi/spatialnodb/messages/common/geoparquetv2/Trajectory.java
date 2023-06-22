package gr.ds.unipi.spatialnodb.messages.common.geoparquetv2;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;

import java.io.Serializable;
import java.util.List;

public class Trajectory implements Serializable {

    private final String objectId;
    private final long trajectoryId;

    private final LineString lineStringWithTime;

    private final double minLongitude;
    private final double minLatitude;
    private final long minTimestamp;
    private final double maxLongitude;
    private final double maxLatitude;
    private final long maxTimestamp;

    @Override
    public String toString() {
        return "Trajectory{" +
                "objectId='" + objectId + '\'' +
                ", trajectoryId=" + trajectoryId +
                ", lineStringWithTime=" + lineStringWithTime.toString() +
                ", minLongitude=" + minLongitude +
                ", minLatitude=" + minLatitude +
                ", minTimestamp=" + minTimestamp +
                ", maxLongitude=" + maxLongitude +
                ", maxLatitude=" + maxLatitude +
                ", maxTimestamp=" + maxTimestamp +
                '}';
    }

    public Trajectory(String objectId, long trajectoryId, LineString lineStringWithTime, double minLongitude, double minLatitude, long minTimestamp, double maxLongitude, double maxLatitude, long maxTimestamp) {
        this.objectId = objectId;
        this.trajectoryId = trajectoryId;
        this.lineStringWithTime = lineStringWithTime;
        this.minLongitude = minLongitude;
        this.minLatitude = minLatitude;
        this.minTimestamp = minTimestamp;
        this.maxLongitude = maxLongitude;
        this.maxLatitude = maxLatitude;
        this.maxTimestamp = maxTimestamp;
//        wkb = new WKBWriter();
    }

    public Trajectory(String objectId, LineString lineStringWithTime, double minLongitude, double minLatitude, long minTimestamp, double maxLongitude, double maxLatitude, long maxTimestamp) {
        this.objectId = objectId;
        this.trajectoryId=-1;
        this.lineStringWithTime = lineStringWithTime;
        this.minLongitude = minLongitude;
        this.minLatitude = minLatitude;
        this.minTimestamp = minTimestamp;
        this.maxLongitude = maxLongitude;
        this.maxLatitude = maxLatitude;
        this.maxTimestamp = maxTimestamp;
//        wkb = new WKBWriter();
    }

    public Trajectory(String objectId, long trajectoryId, List<Trajectory> trajectorySegments) {

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
        this.trajectoryId = trajectoryId;

        int spatioTemporalPointsNum = trajectorySegments.get(0).lineStringWithTime.getCoordinates().length;

        //SpatioTemporalPoint spatioTemporalPoint = trajectorySegments.get(0).getSpatioTemporalPoints()[trajectorySegments.get(0).getSpatioTemporalPoints().length-1];

        for (int i = 1; i < trajectorySegments.size(); i++) {
            spatioTemporalPointsNum = spatioTemporalPointsNum + (trajectorySegments.get(i).lineStringWithTime.getCoordinates().length-1);
//            if(!spatioTemporalPoint.equals(trajectorySegments.get(i).spatioTemporalPoints[0])){
//                try {
//                    throw new Exception("The concatenation of continuous segments is erroneous due to non continuous trajectory segments");
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//            spatioTemporalPoint = trajectorySegments.get(i).getSpatioTemporalPoints()[trajectorySegments.get(i).getSpatioTemporalPoints().length-1];
        }

        Coordinate[] coordinates = new Coordinate[spatioTemporalPointsNum];
        int arrayIndex = 0;

        for (int i = 0; i < trajectorySegments.get(0).lineStringWithTime.getCoordinates().length; i++) {
//            timestamps[arrayIndex] = trajectorySegments.get(0).getTimestamps()[i];
            coordinates[arrayIndex] = new Coordinate(trajectorySegments.get(0).getLineStringWithTime().getCoordinates()[i].x,trajectorySegments.get(0).getLineStringWithTime().getCoordinates()[i].y,trajectorySegments.get(0).getLineStringWithTime().getCoordinates()[i].z);
            arrayIndex++;
        }

        for (int i = 1; i < trajectorySegments.size(); i++) {
            for (int j = 1; j < trajectorySegments.get(i).lineStringWithTime.getCoordinates().length; j++) {
//                timestamps[arrayIndex] = trajectorySegments.get(i).getTimestamps()[j];
                coordinates[arrayIndex] = new Coordinate(trajectorySegments.get(i).getLineStringWithTime().getCoordinates()[j].x,trajectorySegments.get(i).getLineStringWithTime().getCoordinates()[j].y,trajectorySegments.get(i).getLineStringWithTime().getCoordinates()[j].z);
                arrayIndex++;
            }
        }

        this.lineStringWithTime = new GeometryFactory().createLineString(coordinates);

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

    public long getTrajectoryId() {
        return trajectoryId;
    }

//    public byte[] getWKB() {
//        return wkb.write(lineString);
//    }

    public LineString getLineStringWithTime() {
        return lineStringWithTime;
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

}
