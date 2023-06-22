package gr.ds.unipi.spatialnodb.messages.common.segmentv3;

import com.mongodb.client.model.geojson.LineString;
import com.mongodb.client.model.geojson.Position;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Trajectory implements Serializable {

    private final String objectId;
    private final long trajectoryId;

    private final BsonDocument lineString;
    private final long[] timestamps;

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
                ", lineString=" + lineString.toString() +
                ", timestamps=" + Arrays.toString(timestamps) +
                ", minLongitude=" + minLongitude +
                ", minLatitude=" + minLatitude +
                ", minTimestamp=" + minTimestamp +
                ", maxLongitude=" + maxLongitude +
                ", maxLatitude=" + maxLatitude +
                ", maxTimestamp=" + maxTimestamp +
                '}';
    }

    public Trajectory(String objectId, long trajectoryId, BsonDocument lineString, long[] timestamps, double minLongitude, double minLatitude, long minTimestamp, double maxLongitude, double maxLatitude, long maxTimestamp) {
        this.objectId = objectId;
        this.trajectoryId = trajectoryId;
        this.lineString = lineString;
        this.timestamps = timestamps;
        this.minLongitude = minLongitude;
        this.minLatitude = minLatitude;
        this.minTimestamp = minTimestamp;
        this.maxLongitude = maxLongitude;
        this.maxLatitude = maxLatitude;
        this.maxTimestamp = maxTimestamp;
    }

//    public Trajectory(String objectId, BsonDocument lineString, long[] timestamps, double minLongitude, double minLatitude, long minTimestamp, double maxLongitude, double maxLatitude, long maxTimestamp) {
//        this.objectId = objectId;
//        this.trajectoryId=-1;
//        this.lineString = lineString;
//        this.timestamps = timestamps;
//        this.minLongitude = minLongitude;
//        this.minLatitude = minLatitude;
//        this.minTimestamp = minTimestamp;
//        this.maxLongitude = maxLongitude;
//        this.maxLatitude = maxLatitude;
//        this.maxTimestamp = maxTimestamp;
//    }

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

        int spatioTemporalPointsNum = trajectorySegments.get(0).getTimestamps().length;

        //SpatioTemporalPoint spatioTemporalPoint = trajectorySegments.get(0).getSpatioTemporalPoints()[trajectorySegments.get(0).getSpatioTemporalPoints().length-1];

        for (int i = 1; i < trajectorySegments.size(); i++) {
            spatioTemporalPointsNum = spatioTemporalPointsNum + (trajectorySegments.get(i).getTimestamps().length-1);
//            if(!spatioTemporalPoint.equals(trajectorySegments.get(i).spatioTemporalPoints[0])){
//                try {
//                    throw new Exception("The concatenation of continuous segments is erroneous due to non continuous trajectory segments");
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//            spatioTemporalPoint = trajectorySegments.get(i).getSpatioTemporalPoints()[trajectorySegments.get(i).getSpatioTemporalPoints().length-1];
        }

        long[] timestamps = new long[spatioTemporalPointsNum];
        List<Position> coordinates = new ArrayList<>(spatioTemporalPointsNum);
        int arrayIndex = 0;

        for (int i = 0; i < trajectorySegments.get(0).getTimestamps().length; i++) {
            timestamps[arrayIndex] = trajectorySegments.get(0).getTimestamps()[i];
            coordinates.add(new Position(trajectorySegments.get(0).getLineString().toBsonDocument().getArray("coordinates").get(i).asArray().get(0).asDouble().doubleValue(),trajectorySegments.get(0).getLineString().toBsonDocument().getArray("coordinates").get(i).asArray().get(1).asDouble().doubleValue()));
            arrayIndex++;
        }

        for (int i = 1; i < trajectorySegments.size(); i++) {
            for (int j = 1; j < trajectorySegments.get(i).getTimestamps().length; j++) {
                timestamps[arrayIndex] = trajectorySegments.get(i).getTimestamps()[j];
                coordinates.add(new Position(trajectorySegments.get(i).getLineString().toBsonDocument().getArray("coordinates").get(j).asArray().get(0).asDouble().doubleValue(),trajectorySegments.get(i).getLineString().toBsonDocument().getArray("coordinates").get(j).asArray().get(1).asDouble().doubleValue()));
                arrayIndex++;
            }
        }

        this.timestamps = timestamps;
        this.lineString = BsonDocumentWrapper.parse(new LineString(coordinates).toJson());

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

    public BsonDocument getLineString() {
        return lineString;
    }

    public long[] getTimestamps() {
        return timestamps;
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
