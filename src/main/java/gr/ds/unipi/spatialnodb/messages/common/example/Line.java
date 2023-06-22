package gr.ds.unipi.spatialnodb.messages.common.example;

import java.io.Serializable;

public class Line implements Serializable {

    private final double[] vals;
    private final double minX;
    private final double maxX;
    private final double minY;
    private final double maxY;

    public Line(double minX, double maxX, double minY, double maxY, double[] vals) {
        this.minX = minX;
        this.maxX = maxX;
        this.minY = minY;
        this.maxY = maxY;
        this.vals = vals;
    }

    public double[] getVals() { return vals;}

    public double getMinX() {
        return minX;
    }

    public double getMaxX() {
        return maxX;
    }

    public double getMinY() {
        return minY;
    }

    public double getMaxY() {
        return maxY;
    }
}
