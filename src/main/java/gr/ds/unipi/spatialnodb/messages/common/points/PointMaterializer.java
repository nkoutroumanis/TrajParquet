package gr.ds.unipi.spatialnodb.messages.common.points;

import org.apache.parquet.io.api.*;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

import java.util.ArrayList;
import java.util.List;

public class PointMaterializer extends RecordMaterializer<Point> {

    private double x;
    private double y;
    private double t;
    private long hilbertKey;



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
        public void addDouble(double value) {
            x = value;
        }
    };

    PrimitiveConverter p1 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            y = value;
        }
    };

    PrimitiveConverter p2 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            t = value;
        }
    };

    PrimitiveConverter p3 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addLong(long value) {
            hilbertKey = value;
        }
    };


    @Override
    public Point getCurrentRecord() {
        return new Point(x,y,t,hilbertKey);
    }

    @Override
    public GroupConverter getRootConverter() {
        return groupConverter;
    }
}
