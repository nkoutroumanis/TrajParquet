package gr.ds.unipi.spatialnodb.dataloading.points;

import com.typesafe.config.Config;
import gr.ds.unipi.spatialnodb.AppConfig;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
//import gr.ds.unipi.spatialnodb.hadoop.MultipleParquetOutputsFormat;
import gr.ds.unipi.spatialnodb.messages.common.points.Point;
import gr.ds.unipi.spatialnodb.messages.common.points.PointWriteSupport;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.Ranges;
import org.davidmoten.hilbert.SmallHilbertCurve;
import scala.Tuple2;

import java.io.IOException;
import java.text.SimpleDateFormat;


public class DataLoading {

    public static void main(String args[]) throws IOException {

        Config config = AppConfig.newAppConfig(args[0]/*"src/main/resources/app-new.conf"*/).getConfig();

        Config dataLoading = config.getConfig("data-loading");
        final String rawDataPath = dataLoading.getString("rawDataPath");
        final String writePath = dataLoading.getString("writePath");
        final int objectIdIndex = dataLoading.getInt("objectIdIndex");
        final int longitudeIndex = dataLoading.getInt("longitudeIndex");
        final int latitudeIndex = dataLoading.getInt("latitudeIndex");
        final int timeIndex = dataLoading.getInt("timeIndex");
        final String dateFormat = dataLoading.getString("dateFormat");
        final String delimiter = dataLoading.getString("delimiter");

        Config hilbert = dataLoading.getConfig("hilbert");

        final int bits = hilbert.getInt("bits");;
        final double minLon = hilbert.getDouble("minLon");
        final double minLat = hilbert.getDouble("minLat");
        final double minTime = hilbert.getDouble("minTime");
        final double maxLon = hilbert.getDouble("maxLon");
        final double maxLat = hilbert.getDouble("maxLat");
        final double maxTime = hilbert.getDouble("maxTime");

        final SmallHilbertCurve hilbertCurve = HilbertCurve.small().bits(bits).dimensions(3);
        final long maxOrdinates = hilbertCurve.maxOrdinate();

        Job job = Job.getInstance();

        ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        ParquetOutputFormat.setWriteSupportClass(job, PointWriteSupport.class);

        SimpleDateFormat sdf =  new SimpleDateFormat(dateFormat);
        SparkConf sparkConf = new SparkConf()/*.setMaster("local[1]").set("spark.executor.memory","1g")*/.registerKryoClasses(new Class[]{SmallHilbertCurve.class, HilbertUtil.class});
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        Broadcast smallHilbertCurveBr = jsc.broadcast(hilbertCurve);
        long startTime = System.currentTimeMillis();

        JavaPairRDD rdd = jsc.textFile(rawDataPath).map(f->f.split(delimiter)).map(array->new Point(Double.parseDouble(array[longitudeIndex]),Double.parseDouble(array[latitudeIndex]),Double.parseDouble(array[timeIndex]))).mapToPair(point->{


            long[] hil = HilbertUtil.scale3DPoint(point.getX(), minLon, maxLon, point.getY(), minLat, maxLat, point.getT(), minTime, maxTime, maxOrdinates);
            Ranges ranges = ((SmallHilbertCurve)smallHilbertCurveBr.getValue()).query(hil, hil, 0);
            long hilbertValue = ranges.toList().get(0).low();
            return new Tuple2<>(hilbertValue, point);
                }).sortByKey().mapToPair(f->Tuple2.apply( null, new Point(f._2, f._1)));//.mapToPair(f->Tuple2.apply(Tuple2.apply("examp/", null), f._2));

        rdd.saveAsNewAPIHadoopFile(writePath, Void.class, Point.class, ParquetOutputFormat.class, job.getConfiguration());
//        rdd.saveAsNewAPIHadoopFile(writePath, Void.class, Point.class, MultipleParquetOutputsFormat.class, job.getConfiguration());

        long endTime = System.currentTimeMillis();
        System.out.println("Exec Time: "+(endTime-startTime));

    }
}
