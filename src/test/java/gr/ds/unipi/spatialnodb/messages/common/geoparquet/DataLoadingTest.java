package gr.ds.unipi.spatialnodb.messages.common.geoparquet;

import gr.ds.unipi.spatialnodb.messages.common.example.Line;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;


public class DataLoadingTest {

    @Test
    public void test() throws ParseException {
        GeometryFactory g =new GeometryFactory();
        g.createLineString();

        Coordinate[] coordinates = new Coordinate[3];

        coordinates[0] = new Coordinate(100,12);
        coordinates[1] = new Coordinate(123,12);
        coordinates[2] = new Coordinate(324,23465);
        LineString ls = g.createLineString(coordinates);


        System.out.println(ls.toString());

        WKBWriter wkb = new WKBWriter();
        wkb.write(ls);

        WKBReader wkr = new WKBReader();
        Geometry geom = wkr.read(wkb.write(ls));
        for (Coordinate coordinate : geom.getCoordinates()) {
            System.out.println(coordinate);
        }

        System.out.println(geom.toString());
    }
}