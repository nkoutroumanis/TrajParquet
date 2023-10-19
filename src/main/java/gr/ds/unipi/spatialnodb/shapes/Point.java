package gr.ds.unipi.spatialnodb.shapes;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import java.io.ByteArrayOutputStream;
import java.util.Objects;

public class Point {
    private double x;
    private double y;

    public Point() {
        this.x = x;
        this.y = y;
    }

    public Point(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public byte[] toByteArray(Kryo kryo){
        Output output = new Output(new ByteArrayOutputStream(),32);
        kryo.writeObject(output, this);
        byte[] pointByte = output.toBytes();
        return pointByte;
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    public String toString(){
        return "Point ("+x + " "+y+")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Point point = (Point) o;
        return Double.compare(point.x, x) == 0 && Double.compare(point.y, y) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(x, y);
    }
}
