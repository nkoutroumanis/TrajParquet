package gr.ds.unipi.spatialnodb.dataloading;

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

                if((Double.compare(x1,intersectionX) ==0 && Double.compare(y1,intersectionY) ==0)
                        || (Double.compare(x2,intersectionX) ==0 && Double.compare(y2,intersectionY) ==0) ){
//                System.out.println(intersectionX);
//                System.out.println(intersectionY);
                    return false;
                }

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
        if(Double.compare(x,xmax)==-1 &&  Double.compare(x,xmin)!=-1
                && Double.compare(y,ymax)==-1 &&  Double.compare(y,ymin)!=-1){
            return true;
        }
        return false;
    }


    public static Optional<STPoint[]> liangBarsky(double lineX0, double lineY0, long lineT0, double lineX1, double lineY1, long lineT1,
                                         double xMin, double yMin, long tMin, double xMax, double yMax, long tMax){

        double u1 = 0, u2 = 1;

        double dx = lineX1 - lineX0, dy = lineY1 - lineY0;
        long dt = lineT1 - lineT0;

        double p[] = {-dx, dx, -dy, dy, -dt, dt};
        double q[] = {lineX0 - xMin, xMax - lineX0, lineY0 - yMin, yMax - lineY0, lineT0 - tMin, tMax - lineT0};

        for (int i = 0; i < 6; i++) {
            if (p[i] == 0) {
                if (q[i] < 0) {
//                    System.out.println("IT IS NULL 1");
                    return Optional.empty();
                }
            } else {
                double u = (double) q[i] / p[i];
                if (p[i] < 0) {
                    u1 = Math.max(u, u1);
                } else {
                    u2 = Math.min(u, u2);
                }
            }
        }

        if (u1 > u2) {
//            System.out.println("IT IS NULL 2");
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
}