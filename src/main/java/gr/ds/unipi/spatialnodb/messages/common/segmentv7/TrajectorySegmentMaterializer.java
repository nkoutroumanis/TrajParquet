package gr.ds.unipi.spatialnodb.messages.common.segmentv7;

import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import org.apache.parquet.io.api.*;

import java.util.ArrayList;
import java.util.List;

public class TrajectorySegmentMaterializer extends RecordMaterializer<TrajectorySegment[]> {

    private String objectId;

    private long segment;

    private List<TrajectorySegment> trajectoryList;
    private List<SpatioTemporalPoint> currentSpatioTemporalPoints;
    private List<SpatioTemporalPoint> points;


    private double longitude;
    private double latitude;
    private long timestamp;

    private double minLongitude;
    private double minLatitude;
    private long minTimestamp;
    private double maxLongitude;
    private double maxLatitude;
    private long maxTimestamp;

    private final double queryMinLongitude;
    private final double queryMinLatitude;
    private final long queryMinTimestamp;
    private final double queryMaxLongitude;
    private final double queryMaxLatitude;
    private final long queryMaxTimestamp;

    public TrajectorySegmentMaterializer(double xmin, double ymin, long tmin, double xmax, double ymax, long tmax){
        this.queryMinLongitude = xmin;
        this.queryMinLatitude = ymin;
        this.queryMinTimestamp = tmin;
        this.queryMaxLongitude = xmax;
        this.queryMaxLatitude = ymax;
        this.queryMaxTimestamp = tmax;
//        System.out.println("TRAJMAT");
    }

    GroupConverter groupConverter = new GroupConverter() {
        @Override
        public Converter getConverter(int i) {
             if(i==0){
                return p0;
            } else if(i==1){
                return p1;
            } else if(i==2){
                return p2;
            }else if(i ==3){
                return p3;
            } else if(i==4){
                return p4;
            } else if(i==5){
                return p5;
            } else if(i==6){
                return p6;
            } else if(i==7){
                return p7;
            } else if(i==8){
                return p8;
            }
            return null;
        }

        @Override
        public void start() {
            currentSpatioTemporalPoints = new ArrayList<>();
            trajectoryList = new ArrayList<>();
            points = new ArrayList<>();
            //startTime = System.nanoTime();
        }


        @Override
        public void end() {
//            spatioTemporalPoints.clear();
            /*System.out.println("TOTAL TIME: "+(System.nanoTime()-startTime));*/
        }

    };

    PrimitiveConverter p0 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addBinary(Binary value) {
            objectId = value.toStringUsingUTF8();
        }
    };

    PrimitiveConverter p1 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addLong(long value) {
            segment = value;
        }
    };

    GroupConverter p2 = new GroupConverter() {
        @Override
        public Converter getConverter(int i) {
            if(i==0){
                return p2nested0;
            }else if(i==1){
                return p2nested1;
            }else if(i==2){
                return p2nested2;
            }
            return null;
        }

        @Override
        public void start() {
        }

        @Override
        public void end() {
            points.add(new SpatioTemporalPoint(longitude, latitude, timestamp));
//            SpatioTemporalPoint currentSpt = new SpatioTemporalPoint(longitude, latitude, timestamp);
//            if(currentSpatioTemporalPoints.size()>=1){
//                if (currentSpt.getTimestamp() >= queryMinTimestamp && currentSpatioTemporalPoints.get(currentSpatioTemporalPoints.size()-1).getTimestamp() <= queryMaxTimestamp) {
//                    //if one of the points of the line is inside the spatial part of the query
//                    if (HilbertUtil.pointInRectangle(currentSpatioTemporalPoints.get(currentSpatioTemporalPoints.size()-1).getLongitude(), currentSpatioTemporalPoints.get(currentSpatioTemporalPoints.size()-1).getLatitude(), queryMinLongitude, queryMinLatitude, queryMaxLongitude, queryMaxLatitude)
//                            || HilbertUtil.pointInRectangle(currentSpt.getLongitude(), currentSpt.getLatitude(), queryMinLongitude, queryMinLatitude, queryMaxLongitude, queryMaxLatitude)) {
//                        currentSpatioTemporalPoints.add(currentSpt);
//                    } else if (HilbertUtil.lineLineIntersection(currentSpatioTemporalPoints.get(currentSpatioTemporalPoints.size()-1).getLongitude(), currentSpatioTemporalPoints.get(currentSpatioTemporalPoints.size()-1).getLatitude(), currentSpt.getLongitude(), currentSpt.getLatitude(), queryMinLongitude, queryMinLatitude, queryMaxLongitude, queryMinLatitude, true)
//                            || HilbertUtil.lineLineIntersection(currentSpatioTemporalPoints.get(currentSpatioTemporalPoints.size()-1).getLongitude(), currentSpatioTemporalPoints.get(currentSpatioTemporalPoints.size()-1).getLatitude(), currentSpt.getLongitude(), currentSpt.getLatitude(), queryMinLongitude, queryMinLatitude, queryMinLongitude, queryMaxLatitude, true)
//                            || HilbertUtil.lineLineIntersection(currentSpatioTemporalPoints.get(currentSpatioTemporalPoints.size()-1).getLongitude(), currentSpatioTemporalPoints.get(currentSpatioTemporalPoints.size()-1).getLatitude(), currentSpt.getLongitude(), currentSpt.getLatitude(), queryMinLongitude, queryMaxLatitude, queryMaxLongitude, queryMaxLatitude, false)
//                            || HilbertUtil.lineLineIntersection(currentSpatioTemporalPoints.get(currentSpatioTemporalPoints.size()-1).getLongitude(), currentSpatioTemporalPoints.get(currentSpatioTemporalPoints.size()-1).getLatitude(), currentSpt.getLongitude(), currentSpt.getLatitude(), queryMaxLongitude, queryMinLatitude, queryMaxLongitude, queryMaxLatitude, false)) {
//                            //if the line penetrates the spatial part of the query
//                            currentSpatioTemporalPoints.add(currentSpt);
//                    } else {//if the line does not intersect with the spatial part of the query
//                        if(currentSpatioTemporalPoints.size()>=2){
//                            trajectoryList.add(new TrajectorySegment(objectId, segment++, currentSpatioTemporalPoints.toArray(new SpatioTemporalPoint[0]), 0, 0, 0, 0, 0, 0));
//                        }
//                        currentSpatioTemporalPoints.clear();
//                        currentSpatioTemporalPoints.add(currentSpt);
//                    }
//                }else{
//                    if(currentSpatioTemporalPoints.size()>=2){
//                        trajectoryList.add(new TrajectorySegment(objectId, segment++, currentSpatioTemporalPoints.toArray(new SpatioTemporalPoint[0]), 0, 0, 0, 0, 0, 0));
//                    }
//                    currentSpatioTemporalPoints.clear();
//                    currentSpatioTemporalPoints.add(currentSpt);
//                }
//            }else{
//                currentSpatioTemporalPoints.add(currentSpt);
//            }
        }
    };

    PrimitiveConverter p2nested0 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            longitude = value;
        }
    };

    PrimitiveConverter p2nested1 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            latitude = value;
        }
    };

    PrimitiveConverter p2nested2 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addLong(long value) {
            timestamp = value;
        }
    };

    PrimitiveConverter p3 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            minLongitude = value;
        }
    };

    PrimitiveConverter p4 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            minLatitude = value;
        }
    };

    PrimitiveConverter p5 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addLong(long value) {
            minTimestamp = value;
        }
    };

    PrimitiveConverter p6 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            maxLongitude = value;
        }
    };

    PrimitiveConverter p7 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            maxLatitude = value;
        }
    };

    PrimitiveConverter p8 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addLong(long value) {
            maxTimestamp = value;
        }
    };

    @Override
    public TrajectorySegment[] getCurrentRecord() {
        System.out.println("SIZE is "+points.size());
        for (SpatioTemporalPoint currentSpt : points) {
            if(currentSpatioTemporalPoints.size()>=1){
                if (currentSpt.getTimestamp() >= queryMinTimestamp && currentSpatioTemporalPoints.get(currentSpatioTemporalPoints.size()-1).getTimestamp() <= queryMaxTimestamp) {
                    //if one of the points of the line is inside the spatial part of the query
                    if (HilbertUtil.pointInRectangle(currentSpatioTemporalPoints.get(currentSpatioTemporalPoints.size()-1).getLongitude(), currentSpatioTemporalPoints.get(currentSpatioTemporalPoints.size()-1).getLatitude(), queryMinLongitude, queryMinLatitude, queryMaxLongitude, queryMaxLatitude)
                            || HilbertUtil.pointInRectangle(currentSpt.getLongitude(), currentSpt.getLatitude(), queryMinLongitude, queryMinLatitude, queryMaxLongitude, queryMaxLatitude)) {
                        currentSpatioTemporalPoints.add(currentSpt);
                    } else if (HilbertUtil.lineLineIntersection(currentSpatioTemporalPoints.get(currentSpatioTemporalPoints.size()-1).getLongitude(), currentSpatioTemporalPoints.get(currentSpatioTemporalPoints.size()-1).getLatitude(), currentSpt.getLongitude(), currentSpt.getLatitude(), queryMinLongitude, queryMinLatitude, queryMaxLongitude, queryMinLatitude, true)
                            || HilbertUtil.lineLineIntersection(currentSpatioTemporalPoints.get(currentSpatioTemporalPoints.size()-1).getLongitude(), currentSpatioTemporalPoints.get(currentSpatioTemporalPoints.size()-1).getLatitude(), currentSpt.getLongitude(), currentSpt.getLatitude(), queryMinLongitude, queryMinLatitude, queryMinLongitude, queryMaxLatitude, true)
                            || HilbertUtil.lineLineIntersection(currentSpatioTemporalPoints.get(currentSpatioTemporalPoints.size()-1).getLongitude(), currentSpatioTemporalPoints.get(currentSpatioTemporalPoints.size()-1).getLatitude(), currentSpt.getLongitude(), currentSpt.getLatitude(), queryMinLongitude, queryMaxLatitude, queryMaxLongitude, queryMaxLatitude, false)
                            || HilbertUtil.lineLineIntersection(currentSpatioTemporalPoints.get(currentSpatioTemporalPoints.size()-1).getLongitude(), currentSpatioTemporalPoints.get(currentSpatioTemporalPoints.size()-1).getLatitude(), currentSpt.getLongitude(), currentSpt.getLatitude(), queryMaxLongitude, queryMinLatitude, queryMaxLongitude, queryMaxLatitude, false)) {
                        //if the line penetrates the spatial part of the query
                        currentSpatioTemporalPoints.add(currentSpt);
                    } else {//if the line does not intersect with the spatial part of the query
                        if(currentSpatioTemporalPoints.size()>=2){
                            trajectoryList.add(new TrajectorySegment(objectId, segment++, currentSpatioTemporalPoints.toArray(new SpatioTemporalPoint[0]), 0, 0, 0, 0, 0, 0));
                        }
                        currentSpatioTemporalPoints.clear();
                        currentSpatioTemporalPoints.add(currentSpt);
                    }
                }else{
                    if(currentSpatioTemporalPoints.size()>=2){
                        trajectoryList.add(new TrajectorySegment(objectId, segment++, currentSpatioTemporalPoints.toArray(new SpatioTemporalPoint[0]), 0, 0, 0, 0, 0, 0));
                    }
                    currentSpatioTemporalPoints.clear();
                    currentSpatioTemporalPoints.add(currentSpt);
                }
            }else{
                currentSpatioTemporalPoints.add(currentSpt);
            }
        }


        if(currentSpatioTemporalPoints.size()>1){
            trajectoryList.add(new TrajectorySegment(objectId, segment++, currentSpatioTemporalPoints.toArray(new SpatioTemporalPoint[0]), 0, 0, 0, 0, 0, 0));
        }
        if(trajectoryList.size()!=0){
            return trajectoryList.toArray(new TrajectorySegment[0]);
        }
        return null;
    }

    @Override
    public GroupConverter getRootConverter() {
        return groupConverter;
    }
}
