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
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
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

        long[] cube =  hilbertCurve.point(1);

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
    public void checkCase3()  {

        final int bits = 5;
        final double minLon=-9.713331;
        final double minLat=45.001045;
        final long minTime=1443650401000l;
        final double maxLon=-0.015736665;
        final double maxLat=50.7706;
        final long maxTime=1459461603000l;

        final SmallHilbertCurve hilbertCurve = HilbertCurve.small().bits(bits).dimensions(3);
        final long maxOrdinates = hilbertCurve.maxOrdinate();

        double lineX1 = -4.48201;
        double lineY1 = 48.219673;
        long lineT1 = 1453895233000l;

        double lineX2 = -4.71573;
        double lineY2 = 48.298286;
        long lineT2 = 1453895235000l;


        long[] hil1 = HilbertUtil.scaleGeoTemporalPoint(lineX1, minLon, maxLon, lineY1, minLat, maxLat, lineT1, minTime, maxTime, maxOrdinates);
        long[] hil2 = HilbertUtil.scaleGeoTemporalPoint(lineX2, minLon, maxLon, lineY2, minLat, maxLat, lineT2, minTime, maxTime, maxOrdinates);


        System.out.println(hilbertCurve.index(hil1));
        System.out.println(hilbertCurve.index(hil2));


        hilbertCurve.query(hil1,hil2,0).forEach(System.out::println);

        long[] cube =  hilbertCurve.point(20964);
        double xMin = minLon + (cube[0] * (maxLon-minLon)/(maxOrdinates+ 1L));
        double yMin = minLat + (cube[1] * (maxLat-minLat)/(maxOrdinates+ 1L));
        long tMin = minTime + (cube[2] * (maxTime-minTime)/(maxOrdinates+ 1L));

        double xMax = minLon + ((cube[0]+1) * (maxLon-minLon)/(maxOrdinates+ 1L));
        double yMax = minLat + ((cube[1]+1) * (maxLat-minLat)/(maxOrdinates+ 1L));
        long tMax = minTime + ((cube[2]+1) * (maxTime-minTime)/(maxOrdinates+ 1L));


        Optional<STPoint[]> stPoints = HilbertUtil.liangBarsky(lineX1, lineY1, lineT1, lineX2, lineY2, lineT2, xMin, yMin, tMin, xMax, yMax, tMax );

        if(stPoints.isPresent()){
            System.out.println(stPoints.get()[0].getX() +" "+ stPoints.get()[0].getY() +" "+stPoints.get()[0].getT());
            System.out.println(stPoints.get()[1].getX() +" "+ stPoints.get()[1].getY() +" "+stPoints.get()[1].getT());

        }

    }

    @Test
    public void LiangBarsky()  {

        double lineX0 = -4.8645935;
        double lineY0 = 48.490345;
        long lineT0 = 1454445282000000l;

        double lineX1 = -0.8643733;
        double lineY1 = 48.48994;
        long lineT1= 1454445288000000l;

        double xMin = -5.16758365546875;
        double yMin = 48.426718281250004;
        long tMin = 1454026502312000l;

        double xMax = -4.8645338325;
        double yMax = 48.607016875;
        long tMax= 1454520602375000l;


//        double lineX0 = -4.48201;
//        double lineY0 =48.219673;
//        long lineT0 = 1453895233000000l;
//
//        double lineX1 = -4.71573;
//        double lineY1 = 48.298286;
//        long lineT1= 1453895235000000l;

//        double xMin =  -4.8645338325;
//        double yMin = 48.2464196875;
//        long tMin = 1453532402250000l;
//
//        double xMax = -4.5614840095312505;
//        double yMax = 48.426718281250004;
//        long tMax= 1454026502312000l;

//        double xMin =  -4.5614840095312505;
//        double yMin = 48.06612109375;
//        long tMin = 1453532402250000l;
//
//        double xMax = -4.2584341865625;
//        double yMax = 48.2464196875;
//        long tMax= 1454026502312000l;

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
    public void LiangBarskyTest2()  {

        double lineX0 = -2.077968139158168;
        double lineY0 = 49.87955242626847;
        long lineT0 = 1453530800444l;

        double lineX1 = -1.6198933;
        double lineY1 = 50.042137;
        long lineT1= 1453534739000l;

        double xMin = -2.077968139158168;
        double yMin = 48.9932022366616;
        long tMin = 1452645442462l;

        double xMax = -0.5779681391581679;
        double yMax = 50.4932022366616;
        long tMax= 1455237442462l;


        Optional<STPoint[]> stPoints = HilbertUtil.liangBarsky(lineX0, lineY0, lineT0, lineX1, lineY1, lineT1, xMin, yMin, tMin, xMax, yMax, tMax );

        if(stPoints.isPresent()){
            for (STPoint stPoint : stPoints.get()) {
                System.out.println(stPoint);
            }
        }
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

    @Test
    public void hilbertTestPoint3() throws IOException {

        int bits = 7;
        SmallHilbertCurve hilbertCurve = HilbertCurve.small().bits(bits).dimensions(3);
        long maxOrdinates = 1L << bits;

        long[] hil = HilbertUtil.scaleGeoTemporalPoint(-4.4857216, -9.713331, -0.015736665,48.38109 , 45.001045, 50.7706, 1450827290000l, 1443650401000l, 1459461603000l, maxOrdinates);

//        -4.4857216, latitude=48.38109, timestamp=

        Ranges ranges = hilbertCurve.query(hil, hil, 0);
        for (Range range : ranges) {
            System.out.println(range.low() + " - " + range.high());
        }
        long hilbertValue = ranges.toList().get(0).low();
        System.out.println("hilbert: " + hilbertValue);
    }




    @Test
    public void checkCase4()  {

        final int bits = 6;
        final double minLon=19.685170f;
        final double minLat=34.936015f;
        final long minTime=1534280400000L;
        final double maxLon=28.240304f;
        final double maxLat=41.706090f;
        final long maxTime=1535749199994L;

        final SmallHilbertCurve hilbertCurve = HilbertCurve.small().bits(bits).dimensions(3);
        final long maxOrdinates = hilbertCurve.maxOrdinate();

        double lineX1 = 19.685170f;
        double lineY1 = 34.936015f;
        long lineT1 = 1534280400000L;

        double lineX2 = 19.685171f;
        double lineY2 = 34.936016f;
        long lineT2 = 1535749199994L;


        long[] hil1 = HilbertUtil.scaleGeoTemporalPoint(lineX1, minLon, maxLon, lineY1, minLat, maxLat, lineT1, minTime, maxTime, maxOrdinates);
        long[] hil2 = HilbertUtil.scaleGeoTemporalPoint(lineX2, minLon, maxLon, lineY2, minLat, maxLat, lineT2, minTime, maxTime, maxOrdinates);


        System.out.println(hilbertCurve.index(hil1));
        System.out.println(hilbertCurve.index(hil2));

        hilbertCurve.query(hil1,hil2,0).forEach(System.out::println);
    }




    @Test
    public void checkline()  {
        double lineX0 = -1.5356;
        double lineY0 = 50.056896;
        long lineT0 = 1448991819000l;

        double lineX1 = -5.9120865;
        double lineY1 = 48.507065;
        long lineT1= 1449491897000l;

        double xMin = -6.076733124375;
        double yMin = 48.426718281250004;
        long tMin = 1449085501687l;

        double xMax = -5.77368330140625;
        double yMax = 48.607016875;
        long tMax= 1449579601750l;


        Optional<STPoint[]> stPoints = HilbertUtil.liangBarsky(lineX0, lineY0, lineT0, lineX1, lineY1, lineT1, xMin, yMin, tMin, xMax, yMax, tMax );

        if(stPoints.isPresent()){
            for (STPoint stPoint : stPoints.get()) {
                System.out.println(stPoint);
            }
        }

//        System.out.println(HilbertUtil.lineLineIntersection(xMin, yMin, xMin, yMax, lineX0, lineY0, lineX1, lineY1).get().toString());

    }
@Test
    public void checkHilbert(){
        final int bits = 5;
        final double minLon=-9.713331;
        final double minLat=45.001045;
        final long minTime=1443650401000l;
        final double maxLon=-0.015736665;
        final double maxLat=50.7706;
        final long maxTime=1459461603000l;

    double lineX0 = -1.5356;
    double lineY0 = 50.056896;
    long lineT0 = 1448991819000l;

    double lineX1 = -5.9120865;
    double lineY1 = 48.507065;
    long lineT1= 1449491897000l;


    final SmallHilbertCurve hilbertCurve = HilbertCurve.small().bits(bits).dimensions(3);
        final long maxOrdinates = hilbertCurve.maxOrdinate();

    long[] hil1 = HilbertUtil.scaleGeoTemporalPoint(lineX0, minLon, maxLon, lineY0, minLat, maxLat, lineT0, minTime, maxTime, maxOrdinates);
    long[] hil2 = HilbertUtil.scaleGeoTemporalPoint(lineX1, minLon, maxLon, lineY1, minLat, maxLat, lineT1, minTime, maxTime, maxOrdinates);

    System.out.println("hil1 :" +hilbertCurve.index(hil1));
    System.out.println("hil2 :" +hilbertCurve.index(hil2));

    List<Tuple2<Long, STPoint>> myList = new ArrayList<>();

    Ranges ranges = hilbertCurve.query(hil1, hil2, 0);

    for (int k = 0; k < ranges.toList().size(); k++) {
        for (long i = ranges.toList().get(k).low(); i <= ranges.toList().get(k).high(); i++) {


            long[] cube =  hilbertCurve.point(i);

            double xMin = minLon + (cube[0] * (maxLon-minLon)/(maxOrdinates+ 1));
            double yMin = minLat + (cube[1] * (maxLat-minLat)/(maxOrdinates+ 1));
            long tMin = minTime + (cube[2] * (maxTime-minTime)/(maxOrdinates+ 1));

            double xMax = minLon + ((cube[0]+1) * (maxLon-minLon)/(maxOrdinates+ 1));
            double yMax = minLat + ((cube[1]+1) * (maxLat-minLat)/(maxOrdinates+ 1));
            long tMax = minTime + ((cube[2]+1) * (maxTime-minTime)/(maxOrdinates+ 1));

            Optional<STPoint[]> stPoints = HilbertUtil.liangBarsky(lineX0, lineY0, lineT0, lineX1, lineY1, lineT1, xMin, yMin, tMin, xMax, yMax, tMax );

            if(stPoints.isPresent()){

                int j = 0;
                if(Double.compare(stPoints.get()[0].getX(), lineX0)!=0 || Double.compare(stPoints.get()[0].getY(), lineY0)!=0 || stPoints.get()[0].getT() != lineT0){
                    j++;
                }

                if(Double.compare(stPoints.get()[1].getX(), lineX1)!=0 || Double.compare(stPoints.get()[1].getY(), lineY1)!=0 || stPoints.get()[1].getT() != lineT1){
                    j++;
                }

                if(i==12480){
                    System.out.println(stPoints.get()[0]+ " "+stPoints.get()[1]);
                }

//                myList.add(Tuple2.apply());
                System.out.println(i+" "+j);
            }
        }
    }
}


@Test
    public void checkHilbert2(){
        final int bits = 6;
        final double xMin=-8.616597587287732;
        final double yMin=46.958285419481626;
        final long tMin=1457437421647l;
        final double xMax=-6.116597587287731;
        final double yMax=47.958285419481626;
        final long tMax=1457696621647l;


//    double lineX1 = -7.5919822392187495;
//    double lineY1 = 47.75983616307071;
//    long lineT1 = 1457434104838l;
//
//    double lineX2 = -7.704572607663604;
//    double lineY2 = 47.70552390625;
//    long lineT2= 1457449092876l;

    double lineX1 = -3.2154984;
        double lineY1 = 49.871;
        long lineT1 = 1456851507000l;

        double lineX2 = -7.7294984;
        double lineY2 = 47.6935;
        long lineT2= 1457452411000l;

        Optional<STPoint[]> stPoints = HilbertUtil.liangBarsky(lineX1, lineY1, lineT1, lineX2, lineY2, lineT2, xMin, yMin, tMin, xMax, yMax, tMax );

        if(stPoints.isPresent()){
            System.out.println(stPoints.get()[0].getX() +" "+ stPoints.get()[0].getY() +" "+stPoints.get()[0].getT());
            System.out.println(stPoints.get()[1].getX() +" "+ stPoints.get()[1].getY() +" "+stPoints.get()[1].getT());

        }    }




}
