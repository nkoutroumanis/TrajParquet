package gr.ds.unipi.spatialnodb.messages.common.points;

import java.io.Serializable;

public class Point implements Serializable {

    private final double x;
    private final double y;
    private final double t;

    private final long hilbertKey;


    @Override
    public String toString() {
        return "Point{" +
                "x='" + x + '\'' +
                ", y=" + y +
                ", t=" + t +
                ", hilbertKey=" + hilbertKey +
                '}';
    }

    public Point(Point point, long hilbertKey) {
        this.x = point.x;
        this.y = point.y;
        this.t = point.t;
        this.hilbertKey = hilbertKey;
    }

    public Point(double x, double y, double t) {
        this.x = x;
        this.y = y;
        this.t = t;
        this.hilbertKey = -1;
    }

    public Point(double x, double y, double t, long hilbertKey) {
        this.x = x;
        this.y = y;
        this.t = t;
        this.hilbertKey = hilbertKey;
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    public double getT() {
        return t;
    }

    public long getHilbertKey() {
        return hilbertKey;
    }

}
