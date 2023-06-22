package gr.ds.unipi.spatialnodb.messages.common.segment;

import org.apache.parquet.io.api.*;

import java.util.ArrayList;
import java.util.List;

public class TrajectorySegmentMaterializer extends RecordMaterializer<TrajectorySegment> {

    private String objectId;

    private long segment;

    private List<SpatioTemporalPoint> spatioTemporalPoints;

    private double longitude;
    private double latitude;
    private long timestamp;

    private double minLongitude;
    private double minLatitude;
    private long minTimestamp;
    private double maxLongitude;
    private double maxLatitude;
    private long maxTimestamp;

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
            spatioTemporalPoints = new ArrayList<>();
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
            spatioTemporalPoints.add(new SpatioTemporalPoint(longitude, latitude, timestamp));
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
    public TrajectorySegment getCurrentRecord() {
        SpatioTemporalPoint[] sp = new SpatioTemporalPoint[spatioTemporalPoints.size()];
        for (int i = 0; i < spatioTemporalPoints.size(); i++) {
            sp[i] = spatioTemporalPoints.get(i);
        }
        spatioTemporalPoints = null;
        return new TrajectorySegment(objectId, segment, sp, minLongitude, minLatitude, minTimestamp,maxLongitude, maxLatitude, maxTimestamp);
    }

    @Override
    public GroupConverter getRootConverter() {
        return groupConverter;
    }
}
