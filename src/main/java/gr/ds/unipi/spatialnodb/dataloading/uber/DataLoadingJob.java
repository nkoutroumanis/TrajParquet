package gr.ds.unipi.spatialnodb.dataloading.uber;

import com.typesafe.config.Config;
import gr.ds.unipi.spatialnodb.AppConfig;

import java.io.IOException;

public class DataLoadingJob {
    public static void main(String args[]){
        AppConfig config = AppConfig.newAppConfig(args[0]);
        Config loading = config.getConfig().getConfig("data-loading");

        String rawDataPath = loading.getString("rawDataPath");
        String writePath = loading.getString("writePath");
        int longitudeIndex = loading.getInt("longitudeIndex");
        int latitudeIndex = loading.getInt("latitudeIndex");
        int idIndex = loading.getInt("idIndex");
        String delimiter = loading.getString("delimiter");
        String index = loading.getString("index");

        Config u3 = loading.getConfig("U3");
        int resolution = u3.getInt("resolution");

        Config grid = loading.getConfig("Grid");
        double radius = grid.getDouble("radius");
        double minLon = grid.getDouble("minLon");
        double minLat = grid.getDouble("minLat");
        double maxLon = grid.getDouble("maxLon");
        double maxLat = grid.getDouble("maxLat");


        DataLoadingKryo dl = null;

        if(index.equals("U3")){
            dl = new DataLoadingKryo(rawDataPath, writePath, longitudeIndex, latitudeIndex, idIndex, delimiter, resolution);
        }else if(index.equals("Grid")){
            dl = new DataLoadingKryo(rawDataPath, writePath, longitudeIndex, latitudeIndex, idIndex, delimiter, radius, minLon, minLat, maxLon, maxLat);
        }else{
            try {
                throw new Exception("Not correct index has been set");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        try {
            dl.startLoading();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
