package gr.ds.unipi.spatialnodb.messages.common.trajparquet;

//import fi.iki.yak.ts.compression.gorilla.ByteBufferBitInput;
//import fi.iki.yak.ts.compression.gorilla.Decompressor;
//import fi.iki.yak.ts.compression.gorilla.Value;
//import gr.aueb.delorean.chimp.ChimpNDecompressor;
import org.apache.parquet.io.api.*;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class TrajectorySegmentPartialMaterializer extends RecordMaterializer<TrajectorySegmentWithMetadata> {

    private String objectId;
    private long segment;

    private double minLongitude;
    private double minLatitude;
    private long minTimestamp;
    private double maxLongitude;
    private double maxLatitude;
    private long maxTimestamp;

    private byte[] pivotsLongitude;
    private byte[] pivotsLatitude;

    GroupConverter groupConverter = new GroupConverter() {
        @Override
        public Converter getConverter(int i) {
//            if(i==0){
//                return p0;
//            } else if(i==1){
//                return p1;
//            } else if(i==2){
//                return p2;
//            } else if(i==3){
//                return p3;
//            }else if(i ==4){
//                return p4;
//            } else if(i==5){
//                return p5;
//            } else if(i==6){
//                return p6;
//            } else if(i==7){
//                return p7;
//            } else if(i==8){
//                return p8;
//            } else if(i==9){
//                return p9;
//            } else if(i==10){
//                return p10;
//            }
//            return null;
            if(i==0){
                return p0;
            } else if(i==1){
                return p1;
            } else if(i==2){
                return p5;
            } else if(i==3){
                return p6;
            }else if(i ==4){
                return p7;
            } else if(i==5){
                return p8;
            } else if(i==6){
                return p9;
            } else if(i==7){
                return p10;
            } else if(i==8){
                return p11;
            } else if(i==9){
                return p12;
            }
            return null;
        }

        @Override
        public void start() {
        }

        @Override
        public void end() {
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

//    PrimitiveConverter p2 = new PrimitiveConverter() {
//        @Override
//        public boolean isPrimitive() {
//            return super.isPrimitive();
//        }
//
//        @Override
//        public void addBinary(Binary val) {
//
//        }
//    };
//
//    PrimitiveConverter p3 = new PrimitiveConverter() {
//        @Override
//        public boolean isPrimitive() {
//            return super.isPrimitive();
//        }
//
//        @Override
//        public void addBinary(Binary val) {
//
//        }
//    };
//
//    PrimitiveConverter p4 = new PrimitiveConverter() {
//        @Override
//        public boolean isPrimitive() {
//            return super.isPrimitive();
//        }
//
//        @Override
//        public void addBinary(Binary val) {
//
//        }
//    };

    PrimitiveConverter p5 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            minLongitude = value;
        }
    };

    PrimitiveConverter p6 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            minLatitude = value;
        }
    };

    PrimitiveConverter p7 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addLong(long value) {
            minTimestamp = value;
        }
    };

    PrimitiveConverter p8 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            maxLongitude = value;
        }
    };

    PrimitiveConverter p9 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            maxLatitude = value;
        }
    };

    PrimitiveConverter p10 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addLong(long value) {
            maxTimestamp = value;
        }
    };

    PrimitiveConverter p11 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addBinary(Binary val) {
            pivotsLongitude = val.getBytes();
        }
    };

    PrimitiveConverter p12 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addBinary(Binary val) {
            pivotsLatitude = val.getBytes();
        }
    };

    @Override
    public TrajectorySegmentWithMetadata getCurrentRecord() {

        if(pivotsLongitude == null){
            return TrajectorySegmentWithMetadata.newTrajectorySegmentWithMetadata(new TrajectorySegment(objectId, segment, null, minLongitude, minLatitude, minTimestamp,maxLongitude, maxLatitude, maxTimestamp),null);
        }

        ByteBuffer pLongitude = ByteBuffer.wrap(pivotsLongitude);
        ByteBuffer pLatitude = ByteBuffer.wrap(pivotsLatitude);

        SpatialPoint[] spatialPoints = new SpatialPoint[pLongitude.array().length/8];
        for (int i = 0; i < spatialPoints.length; i++) {
            spatialPoints[i] = new SpatialPoint(pLongitude.getDouble(),pLatitude.getDouble());
        }

        pivotsLongitude = null;
        pivotsLatitude = null;
        return TrajectorySegmentWithMetadata.newTrajectorySegmentWithMetadata(new TrajectorySegment(objectId, segment, null, minLongitude, minLatitude, minTimestamp,maxLongitude, maxLatitude, maxTimestamp),spatialPoints);
    }

    @Override
    public GroupConverter getRootConverter() {
        return groupConverter;
    }
}
