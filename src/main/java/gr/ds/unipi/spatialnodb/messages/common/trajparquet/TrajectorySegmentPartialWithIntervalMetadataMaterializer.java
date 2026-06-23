package gr.ds.unipi.spatialnodb.messages.common.trajparquet;

//import fi.iki.yak.ts.compression.gorilla.ByteBufferBitInput;
//import fi.iki.yak.ts.compression.gorilla.Decompressor;
//import fi.iki.yak.ts.compression.gorilla.Value;
//import gr.aueb.delorean.chimp.ChimpNDecompressor;
import gr.ds.unipi.spatialnodb.messages.common.SpatioTemporalPoint;
import org.apache.parquet.io.api.*;

import java.nio.ByteBuffer;

public class TrajectorySegmentPartialWithIntervalMetadataMaterializer extends RecordMaterializer<TrajectorySegmentWithIntervalMetadata> {

    private String objectId;

    private long intervalStart;
    private long intervalStop;

    GroupConverter groupConverter = new GroupConverter() {
        @Override
        public Converter getConverter(int i) {
            if(i==0){
                return p0;
            } else if(i==1){
                return p11;
            } else if(i==2){
                return p12;
            }
            return null;
        }

        @Override
        public void start() {
            intervalStart=0;
            intervalStop=0;
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

//    PrimitiveConverter p1 = new PrimitiveConverter() {
//        @Override
//        public boolean isPrimitive() {
//            return super.isPrimitive();
//        }
//
//        @Override
//        public void addLong(long value) {
//            segment = value;
//        }
//    };
//
//
//    PrimitiveConverter p2 = new PrimitiveConverter() {
//        @Override
//        public boolean isPrimitive() {
//            return super.isPrimitive();
//        }
//
//        @Override
//        public void addBinary(Binary val) {
//            longitude = val.getBytes();
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
//            latitude = val.getBytes();
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
//            timestamp = val.getBytes();
//        }
//    };
//
//    PrimitiveConverter p5 = new PrimitiveConverter() {
//        @Override
//        public boolean isPrimitive() {
//            return super.isPrimitive();
//        }
//
//        @Override
//        public void addDouble(double value) {
//            minLongitude = value;
//        }
//    };
//
//    PrimitiveConverter p6 = new PrimitiveConverter() {
//        @Override
//        public boolean isPrimitive() {
//            return super.isPrimitive();
//        }
//
//        @Override
//        public void addDouble(double value) {
//            minLatitude = value;
//        }
//    };
//
//    PrimitiveConverter p7 = new PrimitiveConverter() {
//        @Override
//        public boolean isPrimitive() {
//            return super.isPrimitive();
//        }
//
//        @Override
//        public void addLong(long value) {
//            minTimestamp = value;
//        }
//    };
//
//    PrimitiveConverter p8 = new PrimitiveConverter() {
//        @Override
//        public boolean isPrimitive() {
//            return super.isPrimitive();
//        }
//
//        @Override
//        public void addDouble(double value) {
//            maxLongitude = value;
//        }
//    };
//
//    PrimitiveConverter p9 = new PrimitiveConverter() {
//        @Override
//        public boolean isPrimitive() {
//            return super.isPrimitive();
//        }
//
//        @Override
//        public void addDouble(double value) {
//            maxLatitude = value;
//        }
//    };
//
//    PrimitiveConverter p10 = new PrimitiveConverter() {
//        @Override
//        public boolean isPrimitive() {
//            return super.isPrimitive();
//        }
//
//        @Override
//        public void addLong(long value) {
//            maxTimestamp = value;
//        }
//    };

    PrimitiveConverter p11 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addLong(long value) {
            intervalStart = value;
        }
    };

    PrimitiveConverter p12 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addLong(long value) {
            intervalStop = value;
        }
    };

    @Override
    public TrajectorySegmentWithIntervalMetadata getCurrentRecord() {
        long[] intervals = null;
        if(intervalStart!= 0L){
            intervals = new long[]{intervalStart, intervalStop};
        }

//        if(objectId.equals("538002828")&& segment==2){
//            System.out.println("Here2 "+ Arrays.toString(intervals));
//        }
        return TrajectorySegmentWithIntervalMetadata.newTrajectorySegmentWithIntervalMetadata(new TrajectorySegment(objectId, -1, null, -1, -1, -1,-1, -1, -1), intervals);
    }

    @Override
    public GroupConverter getRootConverter() {
        return groupConverter;
    }
}
