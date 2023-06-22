package gr.ds.unipi.spatialnodb.messages.common.example;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;

import java.util.ArrayList;
import java.util.List;

public class LineMaterializer extends RecordMaterializer<Line> {

    double minX;
    double maxX;
    double minY;
    double maxY;
    List<Double> vals;

    GroupConverter groupConverter = new GroupConverter() {
        @Override
        public Converter getConverter(int i) {
            if(i==0){
                return p1;
            } else if(i==1){
                return p2;
            } else if(i==2){
                return p3;
            } else if(i==3){
                return p4;
            }else if(i ==4){
                return groupPartConverter;
            }
            return null;
        }

        @Override
        public void start() {
            vals = new ArrayList<>();
        }

        @Override
        public void end() {
            vals.clear();
        }
    };

    GroupConverter groupPartConverter = new GroupConverter() {
        @Override
        public Converter getConverter(int i) {
            if(i==0){
                return p5;
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

    PrimitiveConverter p1 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            minX = value;
        }
    };

    PrimitiveConverter p2 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            maxX = value;
        }
    };

    PrimitiveConverter p3 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            minY = value;
        }
    };

    PrimitiveConverter p4 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            maxY = value;
        }
    };

    PrimitiveConverter p5 = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }

        @Override
        public void addDouble(double value) {
            vals.add(value);
        }

    };

    @Override
    public Line getCurrentRecord() {
        return new Line(minX, maxX, minY, maxY, vals.stream().mapToDouble(Double::doubleValue).toArray());
    }

    @Override
    public GroupConverter getRootConverter() {
        return groupConverter;
    }
}
