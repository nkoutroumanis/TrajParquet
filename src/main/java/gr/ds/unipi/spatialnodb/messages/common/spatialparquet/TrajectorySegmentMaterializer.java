package gr.ds.unipi.spatialnodb.messages.common.spatialparquet;

import org.apache.parquet.io.api.*;

import java.util.ArrayList;
import java.util.List;

public class TrajectorySegmentMaterializer extends RecordMaterializer<TrajectorySegment> {

    private String objectId;

    private long segment;

    private List<SpatioTemporalPoint> spts;
    private double longitude;
    private double latitude;
    private long timestamp;

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
                return p3;
            }
            return null;
        }

        @Override
        public void start() {
            spts=new ArrayList<>();
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
        public void addInt(int val) {
        }
    };

    GroupConverter p3 = new GroupConverter() {
        @Override
        public Converter getConverter(int i) {
            if(i==0){
                return p30;
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

    GroupConverter p30 = new GroupConverter() {
        @Override
        public Converter getConverter(int i) {
            if(i==0){
                return p300;
            }else if(i==1){
                return p301;
            }else if(i==2){
                return p302;
            }
            return null;
        }

        @Override
        public void start() {

        }

        @Override
        public void end() {
            spts.add(new SpatioTemporalPoint(longitude,latitude,timestamp));
        }
    };

    PrimitiveConverter p300 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            longitude = value;
        }
    };

    PrimitiveConverter p301 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            latitude = value;
        }
    };

    PrimitiveConverter p302 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addLong(long value) {
            timestamp = value;
        }
    };

    @Override
    public TrajectorySegment getCurrentRecord() {
        return new TrajectorySegment(objectId, segment, spts.toArray(new SpatioTemporalPoint[0]), -1, -1, -1,-1, -1, -1);
    }

    @Override
    public GroupConverter getRootConverter() {
        return groupConverter;
    }
}
