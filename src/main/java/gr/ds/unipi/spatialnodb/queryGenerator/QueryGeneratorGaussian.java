package gr.ds.unipi.spatialnodb.queryGenerator;

import com.typesafe.config.Config;
import gr.ds.unipi.spatialnodb.AppConfig;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.Random;

public class QueryGeneratorGaussian {

    public static void main(String args[]) throws IOException {

        Config config = AppConfig.newAppConfig(/*args[0]*/"src/main/resources/queryGenerator.conf").getConfig();

        Config dataLoading = config.getConfig("queries");

        final double xLength = dataLoading.getDouble("xLength");
        final double yLength = dataLoading.getDouble("yLength");
        final double timeLength = dataLoading.getLong("timeLength");//seconds
        final double numberOfQueries = dataLoading.getDouble("numberOfQueries");
        final String filePath = dataLoading.getString("filePath");

        final boolean fixedSizes = dataLoading.getBoolean("fixedSizes");

        Config boundingBox = dataLoading.getConfig("bounding-box");

        final double minLon = boundingBox.getDouble("minLon");
        final double minLat = boundingBox.getDouble("minLat");
        final double minTime = boundingBox.getLong("minTime");

        final double maxLon = boundingBox.getDouble("maxLon");
        final double maxLat = boundingBox.getDouble("maxLat");
        final double maxTime = boundingBox.getLong("maxTime");

        FileWriter writer = new FileWriter(filePath);
        Random r = new Random();

        for (int i = 0; i < numberOfQueries; i++) {
            double randomXValue = minLon + (maxLon - minLon) * r.nextDouble();
            double randomYValue = minLat + (maxLat - minLat) * r.nextDouble();
            double randomTValue = minTime + (maxTime - minTime) * r.nextDouble();

            double x1;
            double x2;

            double y1;
            double y2;

            double t1;
            double t2;

            if (fixedSizes) {

                x1 = randomXValue - (xLength / 2);
                x2 = randomXValue + (xLength / 2);

                y1 = randomYValue - (yLength / 2);
                y2 = randomYValue + (yLength / 2);

                t1 = randomTValue - (timeLength / 2);
                t2 = randomTValue + (timeLength / 2);

            }else{
                x1 = randomXValue - ((xLength / 2)*r.nextDouble());
                x2 = randomXValue + ((xLength / 2)*r.nextDouble());

                y1 = randomYValue - ((yLength / 2)*r.nextDouble());
                y2 = randomYValue + ((yLength / 2)*r.nextDouble());

                t1 = randomYValue - (timeLength / 2);
                t2 = randomYValue + (timeLength / 2);
            }

            if (x1 < minLon || x2 > maxLon || y1 < minLat || y2 > maxLat ||
                    t1 < minTime || t2 > maxTime) {
                i--;
            } else {
                writer.write(x1+";"+y1+";"+t1+";"+x2+";"+y2+";"+t2+"\n");
            }
        }
        writer.close();
    }
}
