package gr.ds.unipi.spatialnodb.query;

import com.typesafe.config.Config;
import com.uber.h3core.H3Core;
import gr.ds.unipi.grid.Grid;
import gr.ds.unipi.grid.NewFunc;
import gr.ds.unipi.shapes.Point;
import gr.ds.unipi.shapes.Rectangle;
import gr.ds.unipi.spatialnodb.AppConfig;

import java.io.IOException;

public class SpatialQueryJob {
    public static void main(String args[]) throws IOException {
        AppConfig config = AppConfig.newAppConfig(args[0]);
        Config query = config.getConfig().getConfig("query");

        String path = query.getString("path");
        String type = query.getString("type");

        Config csv = query.getConfig("CSV");
        int longitudeIndex = csv.getInt("longitudeIndex");
        int latitudeIndex = csv.getInt("latitudeIndex");
        String delimiter = csv.getString("delimiter");

        Config parquet = query.getConfig("Parquet");
        String longitudeColumnName = parquet.getString("longitudeColumnName");
        String latitudeColumnName = parquet.getString("latitudeColumnName");

        String index = query.getString("index");

        Config u3 = query.getConfig("U3");
        int resolution = u3.getInt("resolution");

        Config grid = query.getConfig("Grid");
        double radius = grid.getDouble("radius");
        double minLon = grid.getDouble("minLon");
        double minLat = grid.getDouble("minLat");
        double maxLon = grid.getDouble("maxLon");
        double maxLat = grid.getDouble("maxLat");

        Config rectangle = query.getConfig("rectangle");
        Rectangle rectangleQuery = Rectangle.newRectangle(Point.newPoint(rectangle.getDouble("minLon"), rectangle.getDouble("minLat") ), Point.newPoint(rectangle.getDouble("maxLon"), rectangle.getDouble("maxLat")));

        SpatialQuery dl = null;

        if(type.equals("CSV")){
            dl = new SpatialQuery(path, rectangleQuery, longitudeIndex, latitudeIndex, delimiter);
        }else{
            if(index.equals("U3")){
                dl = new SpatialQuery(path, rectangleQuery, longitudeColumnName, latitudeColumnName, H3Core.newInstance());
            }else if(index.equals("Grid")){
                Grid g = Grid.newGeoGrid(Rectangle.newRectangle(Point.newPoint(minLon,minLat),Point.newPoint(maxLon,maxLat)),radius, NewFunc.datasetB);
                dl = new SpatialQuery(path, rectangleQuery, longitudeColumnName, latitudeColumnName, g);
            }else{
                try {
                    throw new Exception("Not correct index has been set");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        dl.startQuerying();
    }
}
