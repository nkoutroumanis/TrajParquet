package gr.ds.unipi.spatialnodb.queries.points;

import com.typesafe.config.Config;
import gr.ds.unipi.spatialnodb.AppConfig;
import gr.ds.unipi.spatialnodb.messages.common.points.Point;
import gr.ds.unipi.spatialnodb.messages.common.points.PointReadSupport;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static org.apache.parquet.filter2.predicate.FilterApi.*;

public class RangeQueriesNoSpark {
    public static void main(String args[]) throws IOException {

        Config config = AppConfig.newAppConfig(args[0]/*"src/main/resources/app-new.conf"*/).getConfig();

        Config dataLoading = config.getConfig("queries");
        final String parquetPath = dataLoading.getString("parquetPath");
        final String queriesFilePath = dataLoading.getString("queriesFilePath");
        final String queriesFileExport = dataLoading.getString("queriesFileExport");

        List<Long> times = new ArrayList<>();

        BufferedWriter bw = new BufferedWriter(new FileWriter(queriesFileExport));
        BufferedReader br = new BufferedReader(new FileReader(queriesFilePath));
        String query;
        while ((query = br.readLine()) != null) {
            String[] queryParts = query.split(";");
            double queryMinLongitude = Double.parseDouble(queryParts[0]);
            double queryMinLatitude = Double.parseDouble(queryParts[1]);
            double queryMinTimestamp = Double.parseDouble(queryParts[2]);

            double queryMaxLongitude = Double.parseDouble(queryParts[3]);
            double queryMaxLatitude = Double.parseDouble(queryParts[4]);
            double queryMaxTimestamp = Double.parseDouble(queryParts[5]);

            FilterPredicate xAxis= and(gtEq(doubleColumn("x"), queryMinLongitude), ltEq(doubleColumn("x"), queryMaxLongitude));
//            FilterPredicate yAxis= and(gtEq(doubleColumn("y"), queryMinLatitude), ltEq(doubleColumn("y"), queryMaxLatitude));
//            FilterPredicate tAxis= and(gtEq(doubleColumn("t"), queryMinTimestamp), ltEq(doubleColumn("t"), queryMaxTimestamp));

            ParquetReader<Point> reader = ParquetReader.builder(new PointReadSupport(), new Path(parquetPath))
                    .withFilter(FilterCompat.get(xAxis))
                    // - don't use record filters for x,y
                    // - only a custom filter for an entire geometry object
                    //   can be used instead.
                    .useRecordFilter(true)
                    .useDictionaryFilter(true)
//                    .useBloomFilter(false)
//                    .useColumnIndexFilter(true)
                    .useSignedStringMinMax(false)
                    // use this to filter column blocks based on range
                    .useStatsFilter(true)
                    .build();

            long startTime = System.currentTimeMillis();
            long num = 0;

            Point geo = reader.read();
            while(geo != null) {
                num++;
                geo = reader.read();
            }

            long endTime = System.currentTimeMillis();
            times.add((endTime - startTime));

            bw.write((endTime - startTime)+";"+num+";"+ DataPage.counter);
            DataPage.counter = 0;
            bw.newLine();
        }

        for (int ind = 0; ind < 10; ind++) {
            times.remove(0);
        }
        bw.write(times.stream().mapToLong(Long::longValue).average().getAsDouble()+"");

        bw.close();
        br.close();
    }
}
