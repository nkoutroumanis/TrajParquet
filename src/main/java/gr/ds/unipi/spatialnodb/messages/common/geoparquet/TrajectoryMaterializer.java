package gr.ds.unipi.spatialnodb.messages.common.geoparquet;

import org.apache.parquet.io.api.*;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

import java.util.ArrayList;
import java.util.List;

public class TrajectoryMaterializer extends RecordMaterializer<Trajectory> {

    private String objectId;
    private long trajectoryId;

    private byte[] wkb;
    private List<Long> timestamps;

    private double minLongitude;
    private double minLatitude;
    private long minTimestamp;
    private double maxLongitude;
    private double maxLatitude;
    private long maxTimestamp;
    //private long startTime;


//    private static final WKBReader wkbReader = new WKBReader();

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
            }else if(i ==4){
                return p4;
            } else if(i==5){
                return p5;
            } else if(i==6){
                return p6;
            } else if(i==7){
                return p7;
            } else if(i==8){
                return p8;
            } else if(i==9){
                return p9;
            }

            return null;
        }

        @Override
        public void start() {
            //startTime = System.nanoTime();
            timestamps = new ArrayList<>();
            trajectoryId=-1;
        }


        @Override
        public void end() {
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
            trajectoryId = value;
        }
    };

    PrimitiveConverter p2 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addBinary(Binary value) {
            wkb = value.getBytes();
        }
    };

    GroupConverter p3 = new GroupConverter() {
        @Override
        public Converter getConverter(int i) {
            if(i==0){
                return p3nested0;
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

    PrimitiveConverter p3nested0 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addLong(long value) {
            timestamps.add(value);
        }
    };

    PrimitiveConverter p4 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            minLongitude = value;
        }
    };

    PrimitiveConverter p5 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            minLatitude = value;
        }
    };

    PrimitiveConverter p6 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addLong(long value) {
            minTimestamp = value;
        }
    };

    PrimitiveConverter p7 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            maxLongitude = value;
        }
    };

    PrimitiveConverter p8 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            maxLatitude = value;
        }
    };

    PrimitiveConverter p9 = new PrimitiveConverter() {
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
    public Trajectory getCurrentRecord() {

        long[] t = new long[timestamps.size()];
        for (int i = 0; i < timestamps.size(); i++) {
            t[i]=timestamps.get(i);
        }

        try {
            return new Trajectory(objectId, trajectoryId, (LineString) new WKBReader().read(wkb), t, minLongitude, minLatitude, minTimestamp,maxLongitude, maxLatitude, maxTimestamp);
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public GroupConverter getRootConverter() {
        return groupConverter;
    }
}
