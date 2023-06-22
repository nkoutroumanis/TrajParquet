package gr.ds.unipi.spatialnodb;

import com.mongodb.client.model.geojson.LineString;
import com.mongodb.client.model.geojson.Position;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.LatLng;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.messages.common.segmentv8.SpatioTemporalPoint;
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

import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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
    public void dateToUnixTimestamp() throws IOException, ParseException {


        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(sdf.parse("2015-10-01 01:00:01").getTime());
        System.out.println(sdf.parse("2016-04-01 01:00:02").getTime());

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

}
