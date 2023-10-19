package gr.ds.unipi.spatialnodb;

import com.mongodb.client.model.geojson.LineString;
import com.mongodb.client.model.geojson.Position;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.LatLng;
import gr.ds.unipi.spatialnodb.shapes.Point;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.messages.common.segmentv8.SpatioTemporalPoint;
import gr.ds.unipi.spatialnodb.shapes.STPoint;
import org.bson.*;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.conversions.Bson;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.BsonOutput;
import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.Range;
import org.davidmoten.hilbert.Ranges;
import org.davidmoten.hilbert.SmallHilbertCurve;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import scala.Tuple3;

import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class HilbertUtilTest {
    @Test
    public void hilbertTestPoint() throws IOException {

        int bits = 3;
        SmallHilbertCurve hilbertCurve = HilbertCurve.small().bits(bits).dimensions(3);
        long maxOrdinates = 1L << bits;

        long[] hil = HilbertUtil.scaleGeoTemporalPoint(50, 0, 100, 50, 0, 100, 1665000000000l, 1660000000000l, 1670000000000l, maxOrdinates);

        Ranges ranges = hilbertCurve.query(hil, hil, 0);
        for (Range range : ranges) {
            System.out.println(range.low() + " - " + range.high());
        }
        long hilbertValue = ranges.toList().get(0).low();
        System.out.println("hilbert: " + hilbertValue);
    }

    @Test
    public void hilbertTestPoint1() throws IOException {

        int bits = 1;
        SmallHilbertCurve hilbertCurve = HilbertCurve.small().bits(bits).dimensions(3);
        long maxOrdinates = 1L << bits;

        long[] hil = HilbertUtil.scaleGeoTemporalPoint(1, 0, 100, 1, 0, 100, 1, 0l, 100l, maxOrdinates);

        Ranges ranges = hilbertCurve.query(hil, hil, 0);
        for (Range range : ranges) {
            System.out.println(range.low() + " - " + range.high());
        }
        long hilbertValue = ranges.toList().get(0).low();
        System.out.println("hilbert: " + hilbertValue);
    }


    @Test
    public void hilbertTestPoint2() throws IOException {

        int bits = 3;
        SmallHilbertCurve hilbertCurve = HilbertCurve.small().bits(bits).dimensions(2);
        long maxOrdinates = 1L << bits;

        long[] hil = HilbertUtil.scaleGeoPoint(15, 0, 100, 15, 0, 100, maxOrdinates);

        System.out.println(hilbertCurve.point(0)[0] +" "+ hilbertCurve.point(0)[1]);

        System.out.println(hilbertCurve.point(1)[0] +" "+ hilbertCurve.point(1)[1]);

        System.out.println(hilbertCurve.point(3)[0] +" "+ hilbertCurve.point(3)[1]);

        //
//        Ranges ranges = hilbertCurve.query(hil, hil, 0);
//        for (Range range : ranges) {
//            System.out.println(range.low() + " - " + range.high());
//        }
//        long hilbertValue = ranges.toList().get(0).low();
//        System.out.println("hilbert: " + hilbertValue);
    }

    @Test
    public void dateToUnixTimestamp() throws IOException, ParseException {


        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("Europe/Athens"));
        System.out.println(sdf.parse("2021-01-01 00:00:00").getTime());
        System.out.println(sdf.parse("2021-01-01 23:59:58").getTime());

    }

    @Test
    public void mongo() throws IOException, ParseException {
        List<Position> coordi = new ArrayList<>();
        coordi.add(new Position(1, 1));
        coordi.add(new Position(2, 2));

        for (BsonValue coordinates : Document.parse(new LineString(coordi).toJson()).toBsonDocument().getArray("coordinates")) {
            BsonArray bsonArray = coordinates.asArray();
            System.out.println(bsonArray.get(0).asDouble());
            System.out.println(bsonArray.get(1).asDouble());
        }


        for (int i = 0; i < Document.parse(new LineString(coordi).toJson()).toBsonDocument().getArray("coordinates").size(); i++) {
            System.out.println(Document.parse(new LineString(coordi).toJson()).toBsonDocument().getArray("coordinates").get(i).asArray().get(0));
        }

        BsonDocument d = BsonDocumentWrapper.parse(new LineString(coordi).toJson());

        BasicOutputBuffer buffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(buffer);
        new BsonDocumentCodec().encode(writer, d, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
        writer.close();
        byte[] a = buffer.toByteArray();

        System.out.println("ARRAYSIZE: " + a.length);
        System.out.println("Result1: " + new RawBsonDocument(buffer.toByteArray()).toBsonDocument());

        BsonReader bsonReader = new BsonBinaryReader(ByteBuffer.wrap(buffer.toByteArray()));
        BsonDocument b = new BsonDocumentCodec().decode(bsonReader, DecoderContext.builder().build());
        bsonReader.close();
        System.out.println("Result2: " + b);


        //new Document();
    }

    @Test
    public void threeDimensionalLineString() throws org.locationtech.jts.io.ParseException {

        Coordinate[] coordinates = new Coordinate[2];
        coordinates[0] = new Coordinate(13.5969, 52.4021, 1181312735047l);
        coordinates[1] = new Coordinate(14.3242, 53.2342, 1181312735090l);

        org.locationtech.jts.geom.LineString ls = new GeometryFactory().createLineString(coordinates);
        System.out.println(ls.getCoordinates()[0].z);

        System.out.println(ls.toString());

        WKBWriter writer = new WKBWriter(3);
        byte[] bytes = writer.write(ls);


        org.locationtech.jts.geom.LineString ls1 = (org.locationtech.jts.geom.LineString) new WKBReader().read(bytes);
        System.out.println(ls1.getCoordinates()[0].z);
    }

    @Test
    public void Bytebuffer() {

        ByteBuffer bb = ByteBuffer.allocate(8*3*10000);
        for (int i = 0; i < 10000; i++) {
            bb.putLong(Double.doubleToLongBits(9928593.30));
            bb.putLong(Double.doubleToRawLongBits(29576.3));
            bb.putLong(8472950695848l);
        }

        ByteBuffer bb1 = ByteBuffer.wrap(bb.array());
        double one = bb1.getDouble();
        double two = bb1.getDouble();
        long three = bb1.getLong();
        System.out.println(one);
        System.out.println(two);
        System.out.println(three);
        System.out.println(bb.position());



//        coordinates[1] = new Coordinate(14.3242, 53.2342, 1181312735090l);
    }

    @Test
    public void benchmarkBytebuffer() {

        ByteBuffer bb = ByteBuffer.allocate(8*3*100000);
        for (int i = 0; i < 100000; i++) {
            bb.putDouble(9928593.30d);
            bb.putDouble(29576.3d);
            bb.putLong(8472950695848l);
        }
        byte[] a = bb.array().clone();

        System.out.println("Length: "+a.length);
        long t1 = System.nanoTime();
        SpatioTemporalPoint[] spts = new SpatioTemporalPoint[100000];
        ByteBuffer bb1 = ByteBuffer.wrap(a);
        for (int i = 0; i < 100000; i++) {
            spts[i] = new SpatioTemporalPoint(bb1.getDouble(), bb1.getDouble(), bb1.getLong());
        }
        System.out.println(System.nanoTime()-t1);
    }

    @Test
    public void benchmarkWKB() throws org.locationtech.jts.io.ParseException {
        Coordinate[] coordinate = new Coordinate[100000];
        for (int i = 0; i < coordinate.length; i++) {
            coordinate[i] = new Coordinate(9928593.30d, 29576.3d, 8472950695848l);
        }

        org.locationtech.jts.geom.LineString l = new GeometryFactory().createLineString(coordinate);
        WKBWriter wkb = new WKBWriter(3);
        byte[] b = wkb.write(l);
        System.out.println("Length: "+b.length);

        long t1 = System.nanoTime();
        new WKBReader().read(b);
        System.out.println(System.nanoTime()-t1);

//        Coordinate[] oArray = o.getCoordinates();
//        for (int i = 0; i < o.getCoordinates().length; i++) {
//            Coordinate coord = oArray[i];
//        }

    }

    @Test
    public void checkImmutability()  {

        List<Integer>[] previous = new ArrayList[2];
        List<Integer>[] current = new ArrayList[2];
        previous[1] = new ArrayList<>();

        System.out.println(previous[1]);

//        previous[0] = 100;
//        previous[1] = 2000;
//        previous[2] = 1;
//
//        current[0] = 593;
//        current[1] = 495;
//        current[2] = 948;
//
//        previous = current;
//
//         current = new long[3];
//        System.out.println(Arrays.toString(current));
//        System.out.println(Arrays.toString(previous));
//
//

    }

    @Test
    public void checkCase()  {
        final int bits = 1;
        final double minLon = 0d;
        final double minLat = 0d;
//        final double minTime = 0d;
        final double maxLon = 100d;
        final double maxLat = 100d;
//        final double maxTime = 100d;

//        final long maxOrdinates = 1L << bits;
        final SmallHilbertCurve hilbertCurve = HilbertCurve.small().bits(bits).dimensions(2);
        final long maxOrdinates = hilbertCurve.maxOrdinate();

//        System.out.println("maxOrdinates "+ maxOrdinates +" "+ hilbertCurve.maxOrdinate());
        long[] hil1 = HilbertUtil.scale2DPoint(0, minLon, maxLon, 50, minLat, maxLat, maxOrdinates);
//        long[] hil2 = HilbertUtil.scale3DPoint(10, minLon, maxLon, 10, minLat, maxLat, 10, minTime, maxTime, maxOrdinates);


        System.out.println("max ordinate: "+hilbertCurve.maxOrdinate());
        System.out.println(Arrays.toString(hil1));
//        System.out.println(Arrays.toString(hil1));

        Ranges ranges = hilbertCurve.query(hil1, hil1, 0);
        List<Range> rangesList = ranges.toList();

        rangesList.stream().forEach(System.out::println);

        System.out.println(hilbertCurve.index(hil1));

        long[] cube =  hilbertCurve.point(3);

        System.out.println("Lower");
        System.out.println( minLon + (cube[0] * (maxLon-minLon)/(maxOrdinates+1l)));
        System.out.println( minLat + (cube[1] * (maxLat-minLat)/(maxOrdinates+1l)));

        System.out.println("Upper");
        System.out.println( minLon + (((cube[0]+1)) * (maxLon-minLon)/(maxOrdinates+1l)));
        System.out.println( minLat + (((cube[1]+1)) * (maxLat-minLat)/(maxOrdinates+1l)));

    }


    @Test
    public void checkCase2()  {
        final int bits = 2;
        final double minLon = 0d;
        final double minLat = 0d;
        final long minTime = 0;
        final double maxLon = 76d;
        final double maxLat = 85d;
        final long maxTime = 97;

//        final long maxOrdinates = 1L << bits;
        final SmallHilbertCurve hilbertCurve = HilbertCurve.small().bits(bits).dimensions(3);
        final long maxOrdinates = hilbertCurve.maxOrdinate();

        double lineX1 = 25;
        double lineY1 = 25;
        long lineT1 = 25;

        double lineX2 = 25;
        double lineY2 = 25;
        long lineT2 = 51;

        long[] hil1 = HilbertUtil.scaleGeoTemporalPoint(lineX1, minLon, maxLon, lineY1, minLat, maxLat, lineT1, minTime, maxTime, maxOrdinates);
        long[] hil2 = HilbertUtil.scaleGeoTemporalPoint(lineX2, minLon, maxLon, lineY2, minLat, maxLat, lineT2, minTime, maxTime, maxOrdinates);


        System.out.println("max ordinate: "+hilbertCurve.maxOrdinate());
        System.out.println(Arrays.toString(hil1));

        Ranges ranges = hilbertCurve.query(hil1, hil2, 0);
        List<Range> rangesList = ranges.toList();

        rangesList.stream().forEach(System.out::println);

        double lonLength = (maxLon-minLon)/(maxOrdinates+1l);
        double latLength = (maxLat-minLat)/(maxOrdinates+1l);

        System.out.println("lonLength "+ lonLength);
        System.out.println("latLength "+ latLength);

        HashSet<Point> intersected = new HashSet<>();

        for (Range range : rangesList) {
            for(long index = range.low(); index<=range.high();index++){
                long[] cube =  hilbertCurve.point(index);

                double xLower = minLon + (cube[0] * (maxLon-minLon)/(maxOrdinates+1l));
                double yLower = minLat + (cube[1] * (maxLat-minLat)/(maxOrdinates+1l));

                double xUpper = xLower + lonLength;
                double yUpper = yLower + latLength;

//                double xUpper = minLon + ((cube[0]+1) * (maxLon-minLon)/(maxOrdinates+1l));
//                double yUpper = minLat + ((cube[1]+1) * (maxLat-minLat)/(maxOrdinates+1l));

                System.out.println("xLower: "+xLower+ " yLower: "+yLower+" xUpper: "+xUpper+ " yUpper: "+yUpper);

                Optional<Point> i1 = HilbertUtil.lineLineIntersection(lineX1, lineY1, lineX2, lineY2, xLower, yLower, xUpper, yLower);
                Optional<Point> i2 = HilbertUtil.lineLineIntersection(lineX1, lineY1, lineX2, lineY2, xLower, yLower, xLower, yUpper);
                Optional<Point> i3 = HilbertUtil.lineLineIntersection(lineX1, lineY1, lineX2, lineY2, xLower, yUpper, xUpper, yUpper);
                Optional<Point> i4 = HilbertUtil.lineLineIntersection(lineX1, lineY1, lineX2, lineY2, xUpper, yLower, xUpper, yUpper);

                i1.ifPresent(intersected::add);
                i2.ifPresent(intersected::add);
                i3.ifPresent(intersected::add);
                i4.ifPresent(intersected::add);
            }
        }




//        intersected.forEach(System.out::println);

        System.out.println("points in intersected set:"+intersected.size());

        intersected.forEach(point -> System.out.println(HilbertUtil.getTime(minLon, minTime, maxLon, maxTime, point.getX())));


        System.out.println("For Y");
        intersected.forEach(point -> System.out.println(HilbertUtil.getTime(minLat, minTime, maxLat, maxTime, point.getY())));



//        System.out.println(hilbertCurve.index(hil1));
//
//        long[] cube =  hilbertCurve.point(3);
//
//        System.out.println("Lower");
//        System.out.println( minLon + (cube[0] * (maxLon-minLon)/(maxOrdinates+1l)));
//        System.out.println( minLat + (cube[1] * (maxLat-minLat)/(maxOrdinates+1l)));
//
//        System.out.println("Upper");
//        System.out.println( minLon + (((cube[0]+1)) * (maxLon-minLon)/(maxOrdinates+1l)));
//        System.out.println( minLat + (((cube[1]+1)) * (maxLat-minLat)/(maxOrdinates+1l)));

        Coordinate[] cubeCoord = new Coordinate[]{new Coordinate(0,0,0), new Coordinate(100,100,100)};
//        org.locationtech.jts.geom.LineString ls = new GeometryFactory().create


        Coordinate[] newCoord = new Coordinate[]{new Coordinate(1,1,1), new Coordinate(2,2,2)};
        org.locationtech.jts.geom.LineString ls = new GeometryFactory().createLineString(newCoord);


    }


    @Test
    public void LiangBarsky()  {

        double lineX0 = 30;
        double lineY0 = 30;
        long lineT0 = 30;

        double lineX1 = 81;
        double lineY1 = 80;
        long lineT1= 80;

        double xMin = 0;
        double yMin = 0;
        long tMin = 0;

        double xMax = 50;
        double yMax = 50;
        long tMax= 50;

        Optional<STPoint[]> stPoints = HilbertUtil.liangBarsky(lineX0, lineY0, lineT0, lineX1, lineY1, lineT1, xMin, yMin, tMin, xMax, yMax, tMax );

        if(stPoints.isPresent()){
            for (STPoint stPoint : stPoints.get()) {
                System.out.println(stPoint);
            }
        }

        //        System.out.println("Executing Liang-Barsky...");
//        double u1 = 0, u2 = 1;
//
//        double dx = lineX1 - lineX0, dy = lineY1 - lineY0;
//        double dt = lineT1 - lineT0;
//
//        double p[] = {-dx, dx, -dy, dy, -dt, dt};
//        double q[] = {lineX0 - xMin, xMax - lineX0, lineY0 - yMin, yMax - lineY0, lineT0 - tMin, tMax - lineT0};
//
//        for (int i = 0; i < 6; i++) {
//            if (p[i] == 0) {
//                if (q[i] < 0) {
////                    return Optional.empty();
////                    System.out.println("IT IS NULL 1");
//                }
//            } else {
//                double u = (double) q[i] / p[i];
//                if (p[i] < 0) {
//                    u1 = Math.max(u, u1);
//                } else {
//                    u2 = Math.min(u, u2);
//                }
//            }
//        }
//        System.out.println("u1: " + u1 + ", u2: " + u2);
//        if (u1 > u2) {
////            return Optional.empty();
//        }
//        double nx0, ny0, nx1, ny1 ;
//        long nt0, nt1;
//        nx0 = (lineX0 + u1 * dx);
//        ny0 = (lineY0 + u1 * dy);
//        nt0 = (long) (lineT0 + u1 * dt);
//
//        nx1 = (lineX0 + u2 * dx);
//        ny1 = (lineY0 + u2 * dy);
//        nt1 = (long) (lineT0 + u2 * dt);
//
//
//        System.out.println(nx0 + " "+ ny0+ " "+ nt0+ " "+ nx1+ " "+ ny1+ " "+ nt1);
//        System.out.println(HilbertUtil.lineLineIntersection(lineX0, lineY0, lineX1, lineY1, 50, 0, 50, 50));
    }
    @Test
    public void checkComparisonCase()  {
        long maxLongitude = Long.MIN_VALUE;
        System.out.println(maxLongitude);
        System.out.println(Long.compare(Long.MIN_VALUE, -4l)==-1);

        System.out.println(Math.round(10.000000111));

        System.out.println(Math.round(10.5));
        System.out.println(Math.round(10.6));

    }

    @Test
    public void checkTuple()  {
        double lineX0 = 48;
        double lineY0 = 48;
        long lineT0 = 48;

        double lineX1 = 55;
        double lineY1 = 55;
        long lineT1= 55;

        double xMin = 0;
        double yMin = 0;
        long tMin = 0;

        double xMax = 100;
        double yMax = 100;
        long tMax= 100;

        Optional<STPoint[]> stPoints = HilbertUtil.liangBarsky(lineX0, lineY0, lineT0, lineX1, lineY1, lineT1, xMin, yMin, tMin, xMax, yMax, tMax );

        if(stPoints.isPresent()){
            for (STPoint stPoint : stPoints.get()) {
                System.out.println(stPoint);
            }
        }

//        System.out.println(HilbertUtil.lineLineIntersection(xMin, yMin, xMin, yMax, lineX0, lineY0, lineX1, lineY1).get().toString());

    }
}
