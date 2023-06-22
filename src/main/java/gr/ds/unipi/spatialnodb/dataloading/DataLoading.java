package gr.ds.unipi.spatialnodb.dataloading;

import gr.ds.unipi.spatialnodb.messages.common.geoparquet.Trajectory;
import gr.ds.unipi.spatialnodb.messages.common.geoparquet.TrajectoryWriteSupport;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.Ranges;
import org.davidmoten.hilbert.SmallHilbertCurve;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.io.WKBWriter;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple8;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class DataLoading {

    private final String rawDataPath;
    private final String writePath;
    private final int objectIdIndex;
    private final int longitudeIndex;
    private final int latitudeIndex;
    private final int timeIndex;
    private final String dataFormat;
    private final String delimiter;
    private final String method;

    private final double minLon;
    private final double minLat;
    private final long minTime;
    private final double maxLon;
    private final double maxLat;
    private final long maxTime;

    private final SmallHilbertCurve hilbertCurve;
    private final long maxOrdinates;

    public DataLoading(String rawDataPath, String writePath, int objectIdIndex, int longitudeIndex, int latitudeIndex, int timeIndex, String dataFormat, String delimiter, String method, int bits, double minLon, double minLat, long minTime, double maxLon, double maxLat, long maxTime) {
        this.rawDataPath = rawDataPath;
        this.writePath = writePath;
        this.objectIdIndex = objectIdIndex;
        this.longitudeIndex = longitudeIndex;
        this.latitudeIndex = latitudeIndex;
        this.timeIndex = timeIndex;
        this.dataFormat = dataFormat;
        this.delimiter = delimiter;
        this.method = method;
        this.maxOrdinates = 1L << bits;
        hilbertCurve = HilbertCurve.small().bits(bits).dimensions(3);

        this.minLon = minLon;
        this.minLat = minLat;
        this.minTime = minTime;
        this.maxLon = maxLon;
        this.maxLat = maxLat;
        this.maxTime = maxTime;
    }

    public void dataLoadingGeoParquet() throws IOException {
        Job job = Job.getInstance();

        ParquetOutputFormat.setCompression(job, CompressionCodecName.GZIP);
        ParquetOutputFormat.setWriteSupportClass(job, TrajectoryWriteSupport.class);
        SimpleDateFormat sdf =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        SimpleDateFormat sdf1 =  null;

        SparkConf sparkConf = new SparkConf().setMaster("local[1]").set("spark.executor.memory","1g");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        JavaPairRDD<Void, Trajectory> rdd = (JavaPairRDD<Void, Trajectory>) jsc.textFile("src/main/resources/trips.csv").map(f->f.split(",")).groupBy(f-> f[0]).mapToPair(f-> {
            List<Tuple3<Double, Double, Long>> tuple = new ArrayList<>();
            for (String[] strings : f._2) {
                long timestamp = -1;
                try{
                    timestamp = sdf.parse(strings[2]).getTime();
                }catch (ParseException p){
                    try{
                        timestamp = sdf1.parse(strings[2]).getTime();
                    }
                    catch (ParseException p1){
                        System.out.println("ERROR");
                    }
                }
                tuple.add(Tuple3.apply(Double.parseDouble(strings[4]), Double.parseDouble(strings[5]), timestamp));
            }
            tuple.sort(Comparator.comparingLong(Tuple3::_3));

            String objectId = f._1;

            double minLongitude = Double.MAX_VALUE;
            double minLatitude = Double.MAX_VALUE;
            long minTimestamp = Long.MAX_VALUE;

            double maxLongitude = Double.MIN_VALUE;
            double maxLatitude = Double.MIN_VALUE;
            long maxTimestamp = Long.MIN_VALUE;

            for (int i = 0; i < tuple.size(); i++) {
                if (Double.compare(minLongitude, tuple.get(i)._1()) == 1) {
                    minLongitude = tuple.get(i)._1();
                }
                if (Double.compare(minLatitude, tuple.get(i)._2()) == 1) {
                    minLatitude = tuple.get(i)._2();
                }
                if (Long.compare(minTimestamp, tuple.get(i)._3()) == 1) {
                    minTimestamp = tuple.get(i)._3();
                }
                if (Double.compare(maxLongitude, tuple.get(i)._1()) == -1) {
                    maxLongitude = tuple.get(i)._1();
                }
                if (Double.compare(maxLatitude, tuple.get(i)._2()) == -1) {
                    maxLatitude = tuple.get(i)._2();
                }
                if (Long.compare(maxTimestamp, tuple.get(i)._3()) == -1) {
                    maxTimestamp = tuple.get(i)._3();
                }
            }

            long[] hil = HilbertUtil.scaleGeoTemporalPoint(tuple.get(0)._1(), minLon, maxLon, tuple.get(0)._2(), minLat, maxLat, tuple.get(0)._3(), minTime, maxTime, maxOrdinates);
            Ranges ranges = hilbertCurve.query(hil, hil, 0);
            long hilbertValue = ranges.toList().get(0).low();

            Tuple2<Long, Tuple8> tup = Tuple2.apply(hilbertValue,Tuple8.apply(objectId, tuple, minLongitude, minLatitude, minTimestamp, maxLongitude, maxLatitude, maxTimestamp));
            return tup;
        }).mapValues(f->{

            Tuple8<String, List<Tuple3<Double, Double, Long>>, Double, Double, Long, Double, Double, Long> tuple8 = f;

            GeometryFactory g =new GeometryFactory();
            Coordinate[] coordinates = new Coordinate[tuple8._2().size()];
            long[] timestamps = new long[tuple8._2().size()];

            for (int i = 0; i < tuple8._2().size(); i++) {
                coordinates[i] = new Coordinate(tuple8._2().get(i)._1(),tuple8._2().get(i)._2());
                timestamps[i] = tuple8._2().get(i)._3();
            }
            LineString ls = g.createLineString(coordinates);
            WKBWriter wkb = new WKBWriter();

            Trajectory trajectory = new Trajectory(tuple8._1(), ls, timestamps, tuple8._3(), tuple8._4(), tuple8._5(), tuple8._6(), tuple8._7(), tuple8._8());
            return trajectory;
        }).sortByKey().mapToPair(f-> new Tuple2<Void, Trajectory>(null, f._2));

        rdd.saveAsNewAPIHadoopFile("src/main/resources/example", Void.class, Trajectory.class, ParquetOutputFormat.class, job.getConfiguration());

    }

    public void dataLoadingParts() throws IOException {
        Job job = Job.getInstance();


    }

    public void dataLoadingSegments() throws IOException {
        Job job = Job.getInstance();


    }

    public static void main(String args[]){

    }
}
