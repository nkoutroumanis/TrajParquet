package gr.ds.unipi.spatialnodb.dataloading;

import gr.ds.unipi.spatialnodb.messages.common.trajparquet.SpatioTemporalPoint;
import gr.ds.unipi.spatialnodb.shapes.Point;
import gr.ds.unipi.spatialnodb.shapes.STPoint;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class HilbertUtil {

//    public static long[] scaleGeoPoint(double lat, double lon, long max) {
//        long x = scale((lat + 90.0) / 180, max);
//        long y = scale((lon + 180.0) / 360, max);
//        return new long[]{y,x};
//    }

    public static long[] scaleGeoPoint(double lon, double minLon, double maxLon, double lat, double minLat, double maxLat,
                                       long max) {
        long x = scale((lon - minLon) / (maxLon - minLon), max);
        long y = scale((lat - minLat) / (maxLat - minLat), max);
        return new long[]{x, y};
    }

    public static long[] scaleGeoTemporalPoint(double lon, double minLon, double maxLon, double lat, double minLat, double maxLat, long time, long minTime, long maxTime,
                                               long max) {
        long x = scale((lon - minLon) / (maxLon - minLon), max);
        long y = scale((lat - minLat) / (maxLat - minLat), max);
        long z = scale(((double) time - minTime) / (maxTime - minTime), max);
        return new long[]{x, y, z};
    }

    public static long[] scale3DPoint(double lon, double minLon, double maxLon, double lat, double minLat, double maxLat, double time, double minTime, double maxTime,
                                               long max) {
//        System.out.println("lon:"+lon +" minLon:"+ minLon +" maxLon:"+ maxLon +" lat:"+ lat +" minLat:"+ minLat +" maxLat:"+ maxLat +" time:"+ time +" minTime:"+ minTime +" maxTime:"+ maxTime +" max:"+ max);
        long x = scale((lon - minLon) / (maxLon - minLon), max);
        long y = scale((lat - minLat) / (maxLat - minLat), max);
        long z = scale((time - minTime) / (maxTime - minTime), max);
        return new long[]{x, y, z};
    }

    public static long[] scale2DPoint(double lon, double minLon, double maxLon, double lat, double minLat, double maxLat, long max) {
//        System.out.println("lon:"+lon +" minLon:"+ minLon +" maxLon:"+ maxLon +" lat:"+ lat +" minLat:"+ minLat +" maxLat:"+ maxLat +" time:"+ time +" minTime:"+ minTime +" maxTime:"+ maxTime +" max:"+ max);
        long x = scale((lon - minLon) / (maxLon - minLon), max);
        long y = scale((lat - minLat) / (maxLat - minLat), max);
        return new long[]{x, y};
    }



    private static long scale(double d, long max) {
        if (!(Double.compare(d, 0) != -1 && Double.compare(d, 1) != 1)) {
            throw new IllegalArgumentException();
        }

        if (d == 1) {
            return max;
        } else {
            return Math.round(Math.floor(d * (max + 1)));
        }
    }

    public static boolean lineLineIntersection(double x1, double y1, double x2, double y2, double x3, double y3, double x4, double y4, boolean touching) {
        // calculate the direction of the lines
        double uA = ((x4-x3)*(y1-y3) - (y4-y3)*(x1-x3)) / ((y4-y3)*(x2-x1) - (x4-x3)*(y2-y1));
        double uB = ((x2-x1)*(y1-y3) - (y2-y1)*(x1-x3)) / ((y4-y3)*(x2-x1) - (x4-x3)*(y2-y1));

        // if uA and uB are between 0-1, lines are colliding
        if (uA >= 0 && uA <= 1 && uB >= 0 && uB <= 1) {

            //
            if(!touching){
                double intersectionX = x1 + (uA * (x2-x1));
                double intersectionY = y1 + (uA * (y2-y1));

                //                System.out.println(intersectionX);
                //                System.out.println(intersectionY);
                return (Double.compare(x1, intersectionX) != 0 || Double.compare(y1, intersectionY) != 0)
                        && (Double.compare(x2, intersectionX) != 0 || Double.compare(y2, intersectionY) != 0);

            }
            return true;
        }
        return false;
    }

    public static Optional<Point> lineLineIntersection(double x1, double y1, double x2, double y2, double x3, double y3, double x4, double y4) {
        // calculate the direction of the lines
        double uA = ((x4-x3)*(y1-y3) - (y4-y3)*(x1-x3)) / ((y4-y3)*(x2-x1) - (x4-x3)*(y2-y1));
        double uB = ((x2-x1)*(y1-y3) - (y2-y1)*(x1-x3)) / ((y4-y3)*(x2-x1) - (x4-x3)*(y2-y1));

        // if uA and uB are between 0-1, lines are colliding
        if (uA >= 0 && uA <= 1 && uB >= 0 && uB <= 1) {
            double intersectionX = x1 + (uA * (x2-x1));
            double intersectionY = y1 + (uA * (y2-y1));
            return Optional.of(new Point(intersectionX,intersectionY));
        }
        return Optional.empty();
    }

    public static long getTime(double x1, long t1, double x2, long t2, double x) {
        return (long) (((t2-t1)/(x2-x1))*(x-x1))+t1;
    }


    public static boolean pointInRectangle(double x, double y, double xmin, double ymin, double xmax, double ymax) {
        return Double.compare(x, xmax) == -1 && Double.compare(x, xmin) != -1
                && Double.compare(y, ymax) == -1 && Double.compare(y, ymin) != -1;
    }


    public static Optional<STPoint[]> liangBarsky(double lineX0, double lineY0, long lineT0, double lineX1, double lineY1, long lineT1,
                                         double xMin, double yMin, long tMin, double xMax, double yMax, long tMax){

        double u1 = 0, u2 = 1;

        double dx = lineX1 - lineX0, dy = lineY1 - lineY0;
        long dt = lineT1 - lineT0;

        double[] p = {-dx, dx, -dy, dy, -dt, dt};
        double[] q = {lineX0 - xMin, xMax - lineX0, lineY0 - yMin, yMax - lineY0, lineT0 - tMin, tMax - lineT0};

        for (int i = 0; i < 6; i++) {
            if (p[i] == 0) {
                if (q[i] < 0) {
                    return Optional.empty();
                }
            } else {
                double u = q[i] / p[i];
                if (p[i] < 0) {
                    u1 = Math.max(u, u1);
                } else {
                    u2 = Math.min(u, u2);
                }
            }
        }

        if (u1 > u2) {
            return Optional.empty();
        }
        double nx0, ny0, nx1, ny1 ;
        long nt0, nt1;
        nx0 = (lineX0 + u1 * dx);
        ny0 = (lineY0 + u1 * dy);
        nt0 = (long) (lineT0 + u1 * dt);

        nx1 = (lineX0 + u2 * dx);
        ny1 = (lineY0 + u2 * dy);
        nt1 = (long) (lineT0 + u2 * dt);

        List<STPoint> stPoints = new ArrayList<>(2);
        STPoint stPoint1 =  new STPoint(nx0, ny0, nt0);
        STPoint stPoint2 =  new STPoint(nx1, ny1, nt1);

//        if(!(new STPoint(lineX0, lineY0, lineT0).equals(stPoint1)) && !(lineT0 == stPoint1.getT())){
            stPoints.add(stPoint1);
//        }

//        if(!(new STPoint(lineX1, lineY1, lineT1).equals(stPoint2)) && !(lineT1 == stPoint2.getT())){
            stPoints.add(stPoint2);
//        }

        if(stPoints.size()==0){
            try {
                throw new Exception("CANNOT BE EMPTY");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
//            return Optional.empty();
        }
        return Optional.of(stPoints.toArray(new STPoint[stPoints.size()]));

    }

    public static boolean doesTrajectoryIntersectWithCube(SpatioTemporalPoint[] spt, double xMin, double yMin, double xMax, double yMax){
        for (int i = 0; i < spt.length-1; i++) {
            if(doesLineIntersectWithCube(spt[i].getLongitude(), spt[i].getLatitude(),spt[i+1].getLongitude(), spt[i+1].getLatitude(), xMin, yMin, xMax, yMax )){
                return true;
            }
        }
        return false;
    }

    public static boolean isTrajectoryDistanceLessThanEpsilonToCube(SpatioTemporalPoint[] spt, double xMin, double yMin, double xMax, double yMax, double epsilon){
        for (int i = 0; i < spt.length-1; i++) {
            if(Double.compare(minDistSegmentToRectangle(spt[i].getLongitude(), spt[i].getLatitude(),spt[i+1].getLongitude(), spt[i+1].getLatitude(), xMin, yMin, xMax, yMax ), epsilon) != 1){
                return true;
            }
        }
        return false;
    }

    public static boolean doesLineIntersectWithCube(double lineX0, double lineY0, double lineX1, double lineY1,
                                                    double xMin, double yMin, double xMax, double yMax){
        double u1 = 0, u2 = 1;

        double dx = lineX1 - lineX0, dy = lineY1 - lineY0;

        double[] p = {-dx, dx, -dy, dy};
        double[] q = {lineX0 - xMin, xMax - lineX0, lineY0 - yMin, yMax - lineY0};

        for (int i = 0; i < 4; i++) {
            if (p[i] == 0) {
                if (q[i] < 0) {
                    return false;
                }
            } else {
                double u = q[i] / p[i];
                if (p[i] < 0) {
                    u1 = Math.max(u, u1);
                } else {
                    u2 = Math.min(u, u2);
                }
            }
        }

        if (u1 > u2) {
            return false;
        }
        return true;
    }

    public static boolean doesLineIntersectWithCube(double lineX0, double lineY0, long lineT0, double lineX1, double lineY1, long lineT1,
                                                  double xMin, double yMin, long tMin, double xMax, double yMax, long tMax){
        double u1 = 0, u2 = 1;

        double dx = lineX1 - lineX0, dy = lineY1 - lineY0;
        long dt = lineT1 - lineT0;

        double[] p = {-dx, dx, -dy, dy, -dt, dt};
        double[] q = {lineX0 - xMin, xMax - lineX0, lineY0 - yMin, yMax - lineY0, lineT0 - tMin, tMax - lineT0};

        for (int i = 0; i < 6; i++) {
            if (p[i] == 0) {
                if (q[i] < 0) {
                    return false;
                }
            } else {
                double u = q[i] / p[i];
                if (p[i] < 0) {
                    u1 = Math.max(u, u1);
                } else {
                    u2 = Math.min(u, u2);
                }
            }
        }

        if (u1 > u2) {
            return false;
        }
        return true;
    }

    public static boolean inBox(double x, double y, long t, double xMin, double yMin, long tMin, double xMax, double yMax, long tMax){
        return Double.compare(x, xMin) != -1 && Double.compare(x, xMax) != 1 &&
                Double.compare(y, yMin) != -1 && Double.compare(y, yMax) != 1 &&
                Long.compare(t, tMin) != -1 && Long.compare(t, tMax) != 1;
    }

    public static double euclideanDistance(double x1, double y1, double x2, double y2) {
        return (Math.sqrt(Math.pow(x2 - x1, 2) + Math.pow(y2 - y1, 2)));
    }

    public static double frechetDistance(SpatioTemporalPoint[] spt1, SpatioTemporalPoint[] spt2) {
            double[] arr = new double[spt1.length];
            arr[0] = HilbertUtil.euclideanDistance(spt1[0].getLongitude(), spt1[0].getLatitude(), spt2[0].getLongitude(), spt2[0].getLatitude());
            for (int i = 1; i < spt1.length; i++) {
                arr[i] = Math.max(arr[i-1],HilbertUtil.euclideanDistance(spt1[i].getLongitude(), spt1[i].getLatitude(), spt2[0].getLongitude(), spt2[0].getLatitude()));
            }

            double diagonal;
            double value;
            for (int j = 1; j < spt2.length; j++) {
                diagonal = arr[0];
                arr[0] = Math.max(HilbertUtil.euclideanDistance(spt1[0].getLongitude(), spt1[0].getLatitude(), spt2[j].getLongitude(), spt2[j].getLatitude()),diagonal);

                for (int i = 1; i < spt1.length; i++) {
                    value = Math.max(HilbertUtil.euclideanDistance(spt1[i].getLongitude(), spt1[i].getLatitude(), spt2[j].getLongitude(), spt2[j].getLatitude()) ,Math.min(arr[i-1],Math.min(arr[i],diagonal)));
                    diagonal = arr[i];
                    arr[i] = value;
                }
            }
            return arr[spt1.length-1];
    }

    //assumes that the segment does not intersect with rectangle
    public static double minDistSegmentToRectangle(double x1, double y1, double x2, double y2,  double xMin, double yMin, double xMax, double yMax){
        double dist = Double.MAX_VALUE;

        // Endpoints of segment to rectangle
        dist = Math.min(dist, minDistPointToRectangle(x1, y1, xMin, yMin, xMax, yMax));
        dist = Math.min(dist, minDistPointToRectangle(x2, y2, xMin, yMin, xMax, yMax));

        // Rectangle corners to segment
        dist = Math.min(dist, minDistPointToSegment(xMin, yMin, x1, y1, x2, y2));
        dist = Math.min(dist, minDistPointToSegment(xMax, yMin, x1, y1, x2, y2));
        dist = Math.min(dist, minDistPointToSegment(xMin, yMax, x1, y1, x2, y2));
        dist = Math.min(dist, minDistPointToSegment(xMax, yMax, x1, y1, x2, y2));

        return dist;
    }

    static double minDistPointToSegment(double px, double py, double x1, double y1, double x2, double y2) {
        double dx = x2 - x1, dy = y2 - y1;
        if (dx == 0 && dy == 0) return Math.sqrt(Math.pow(px - x1, 2) + Math.pow(py - y1, 2));

        double t = ((px - x1) * dx + (py - y1) * dy) / (dx * dx + dy * dy);
        t = Math.max(0, Math.min(1, t));
        double projX = x1 + t * dx, projY = y1 + t * dy;
        return Math.sqrt(Math.pow(px - projX, 2) + Math.pow(py - projY, 2));
    }


    public static double minDistPointToRectangle(double px, double py, double xMin, double yMin, double xMax, double yMax) {
        double dx = Math.max(Math.max(xMin - px, 0), px - xMax);
        double dy = Math.max(Math.max(yMin - py, 0), py - yMax);
        return Math.sqrt(dx * dx + dy * dy);
    }

    //points distance to cube
    public static boolean isMinDistGreaterThan(double xMin, double yMin, double xMax, double yMax, SpatioTemporalPoint[] spatioTemporalPoints, double epsilon) {
        double minDist = Double.MAX_VALUE;
        for (SpatioTemporalPoint spatioTemporalPoint : spatioTemporalPoints) {
            minDist = Double.min(minDist, minDistPointToRectangle(spatioTemporalPoint.getLongitude(), spatioTemporalPoint.getLatitude(), xMin, yMin, xMax, yMax));
            if(Double.compare(minDist, epsilon) != 1){return false;}
        }
        return true;
    }
}
