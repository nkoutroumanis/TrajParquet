package gr.ds.unipi.spatialnodb.dataloading;

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
    public static boolean pointInRectangle(double x, double y, double xmin, double ymin, double xmax, double ymax) {
        if(Double.compare(x,xmax)==-1 &&  Double.compare(x,xmin)!=-1
                && Double.compare(y,ymax)==-1 &&  Double.compare(y,ymin)!=-1){
            return true;
        }
        return false;
    }
}