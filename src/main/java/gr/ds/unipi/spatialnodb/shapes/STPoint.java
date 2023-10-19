package gr.ds.unipi.spatialnodb.shapes;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import java.io.ByteArrayOutputStream;
import java.util.Objects;

public class STPoint {
    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    public long getT() {
        return t;
    }

    private double x;
    private double y;
    private long t;

    public STPoint() {
        this.x = x;
        this.y = y;
        this.t = t;
    }

    public STPoint(double x, double y, long t) {
        this.x = x;
        this.y = y;
        this.t = t;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        STPoint stPoint = (STPoint) o;
        return Double.compare(stPoint.x, x) == 0 && Double.compare(stPoint.y, y) == 0 && t == stPoint.t;
    }

    @Override
    public int hashCode() {
        return Objects.hash(x, y, t);
    }

    @Override
    public String toString() {
        return "STPoint{" +
                "x=" + x +
                ", y=" + y +
                ", t=" + t +
                '}';
    }
}
