package gr.ds.unipi.spatialnodb.messages.common.segmentv8;

import org.apache.parquet.io.api.*;

import java.nio.ByteBuffer;

public class TrajectorySegmentMaterializer extends RecordMaterializer<TrajectorySegment> {

    private String objectId;

    private long segment;

    private byte[] spt;

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
            } else if(i==3){
                return p5;
            }else if(i ==4){
                return p6;
            } else if(i==5){
                return p7;
            } else if(i==6){
                return p8;
            } else if(i==7){
                return p9;
            } else if(i==8){
                return p10;
            }
            return null;
        }

        @Override
        public void start() {
//            timestamp=new ArrayList<>();
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


    PrimitiveConverter p2 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addBinary(Binary val) {
            spt = val.getBytes();
        }
    };

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

    @Override
    public TrajectorySegment getCurrentRecord() {
        ByteBuffer bspt = ByteBuffer.wrap(spt);
        SpatioTemporalPoint[] spatioTemporalPoints = new SpatioTemporalPoint[bspt.array().length/(24)];
        for (int i = 0; i < spatioTemporalPoints.length; i++) {
            spatioTemporalPoints[i] = new SpatioTemporalPoint(bspt.getDouble(),bspt.getDouble(),bspt.getLong());
        }
        return new TrajectorySegment(objectId, segment, spatioTemporalPoints, minLongitude, minLatitude, minTimestamp,maxLongitude, maxLatitude, maxTimestamp);
    }

    @Override
    public GroupConverter getRootConverter() {
        return groupConverter;
    }
}
