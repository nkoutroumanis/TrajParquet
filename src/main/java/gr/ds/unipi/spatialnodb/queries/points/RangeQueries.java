package gr.ds.unipi.spatialnodb.queries.points;

import com.typesafe.config.Config;
import gr.ds.unipi.spatialnodb.AppConfig;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.messages.common.points.Point;
import gr.ds.unipi.spatialnodb.messages.common.points.PointReadSupport;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.Range;
import org.davidmoten.hilbert.Ranges;
import org.davidmoten.hilbert.SmallHilbertCurve;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import scala.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.apache.parquet.filter2.predicate.FilterApi.*;
import static org.apache.parquet.filter2.predicate.FilterApi.and;

public class RangeQueries {
    public static void main(String args[]) throws IOException {

        Config config = AppConfig.newAppConfig(args[0]/*"src/main/resources/app-new.conf"*/).getConfig();

        Config dataLoading = config.getConfig("queries");
        final String parquetPath = dataLoading.getString("parquetPath");
        final String queriesFilePath = dataLoading.getString("queriesFilePath");
        final String queriesFileExport = dataLoading.getString("queriesFileExport");

        Config hilbert = dataLoading.getConfig("hilbert");

        final int bits = hilbert.getInt("bits");;
        final double minLon = hilbert.getDouble("minLon");
        final double minLat = hilbert.getDouble("minLat");
        final double minTime = hilbert.getDouble("minTime");
        final double maxLon = hilbert.getDouble("maxLon");
        final double maxLat = hilbert.getDouble("maxLat");
        final double maxTime = hilbert.getDouble("maxTime");

        final long maxOrdinates = 1L << bits;
        final SmallHilbertCurve hilbertCurve = HilbertCurve.small().bits(bits).dimensions(3);

        Job job = Job.getInstance();

        ParquetInputFormat.setReadSupportClass(job, PointReadSupport.class);

        SparkConf sparkConf = new SparkConf().setMaster("local[1]").set("spark.executor.memory","1g");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

//        Broadcast smallHilbertCurveBr = jsc.broadcast(hilbertCurve);

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
            FilterPredicate yAxis= and(gtEq(doubleColumn("y"), queryMinLatitude), ltEq(doubleColumn("y"), queryMaxLatitude));
            FilterPredicate tAxis= and(gtEq(doubleColumn("t"), queryMinTimestamp), ltEq(doubleColumn("t"), queryMaxTimestamp));

            FilterPredicate fpActualValues = and(tAxis, and(xAxis, yAxis));

            long[] hil1 = HilbertUtil.scale3DPoint(queryMinLongitude, minLon, maxLon, queryMinLatitude, minLat, maxLat, queryMinTimestamp, minTime, maxTime, maxOrdinates);
            long[] hil2 = HilbertUtil.scale3DPoint(queryMaxLongitude, minLon, maxLon, queryMaxLatitude, minLat, maxLat, queryMaxTimestamp, minTime, maxTime, maxOrdinates);


//            System.out.println(queryMinLongitude +" "+ minLon+" "+ maxLon+" "+ queryMinLatitude+" "+ minLat+" "+ maxLat+" "+ queryMinTimestamp+" "+ minTime+" "+ maxTime+" "+ maxOrdinates);
//            System.out.println(queryMaxLongitude+" "+ minLon+" "+ maxLon+" "+ queryMaxLatitude+" "+ minLat+" "+ maxLat+" "+ queryMaxTimestamp+" "+ minTime+" "+ maxTime+" "+ maxOrdinates);

            Ranges ranges = hilbertCurve.query(hil1, hil2, 0);
            List<Range> rangesList = ranges.toList();

            FilterPredicate fp = and(gtEq(longColumn("hilbertKey"), rangesList.get(0).low()), ltEq(longColumn("hilbertKey"), rangesList.get(0).high()));
            for (int r = 1; r < rangesList.size(); r++) {
                fp = or(and(gtEq(longColumn("hilbertKey"), rangesList.get(r).low()), ltEq(longColumn("hilbertKey"), rangesList.get(r).high())),fp);
            }

            ParquetInputFormat.setFilterPredicate(job.getConfiguration(), fp);

//            ParquetInputFormat.setFilterPredicate(job.getConfiguration(), xAxis);

            long startTime = System.currentTimeMillis();
            JavaPairRDD<Void, Point> pairRDD = (JavaPairRDD<Void, Point>) jsc.newAPIHadoopFile(parquetPath,ParquetInputFormat.class, Void.class, Point.class,job.getConfiguration());//.flatMapValues(f->{
            JavaPairRDD<Void, Point> pairRDDRangeQuery = (JavaPairRDD<Void, Point>) pairRDD;
            List<Tuple2<Void, Point>> trajs = pairRDDRangeQuery.collect();

            long num = trajs.size();

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
