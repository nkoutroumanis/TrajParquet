package gr.ds.unipi.spatialnodb.dataloading.geoparquet;

import com.typesafe.config.Config;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.messages.common.*;
import gr.ds.unipi.spatialnodb.messages.common.geoparquet.Trajectory;
import gr.ds.unipi.spatialnodb.messages.common.geoparquet.TrajectoryWriteSupport;
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
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import scala.Tuple2;
import scala.Tuple3;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static gr.ds.unipi.spatialnodb.AppConfig.loadConfig;

public class DataLoading {

    public static void main(String args[]) throws IOException {

        Config config = loadConfig("data-loading.conf");

        Config dataLoading = config.getConfig("data-loading");
        final String rawDataPath = dataLoading.getString("rawDataPath");
        final String writePath = dataLoading.getString("writePath");
        final int objectIdIndex = dataLoading.getInt("objectIdIndex");
        final int longitudeIndex = dataLoading.getInt("longitudeIndex");
        final int latitudeIndex = dataLoading.getInt("latitudeIndex");
        final int timeIndex = dataLoading.getInt("timeIndex");
        final String dateFormat = dataLoading.getString("dateFormat");
        final String delimiter = dataLoading.getString("delimiter");
        final String metricsPathExport = dataLoading.getString("metricsPathExport");
        final String indexType = dataLoading.getString("indexType");
        final IndexUtils indexUtils;
        if(!(indexType.equals("2D") || indexType.equals("3D"))) {
            throw new IllegalArgumentException("The index parameter must be either 2D or 3D");
        }

        Config hilbert = dataLoading.getConfig("hilbert");

        final int bits = hilbert.getInt("bits");;
//        final double minLon = hilbert.getDouble("minLon");
//        final double minLat = hilbert.getDouble("minLat");
//        final long minTime = hilbert.getLong("minTime");
//        final double maxLon = hilbert.getDouble("maxLon");
//        final double maxLat = hilbert.getDouble("maxLat");
//        final long maxTime = hilbert.getLong("maxTime");

        final SmallHilbertCurve hilbertCurve = HilbertCurve.small().bits(bits).dimensions(indexType.equals("3D")?3:2);
        final long maxOrdinates = hilbertCurve.maxOrdinate();

        Job job = Job.getInstance();

        ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);

        ParquetOutputFormat.setWriteSupportClass(job, TrajectoryWriteSupport.class);

        SimpleDateFormat sdf =  new SimpleDateFormat(dateFormat);
        SparkConf sparkConf = new SparkConf().registerKryoClasses(new Class[]{SmallHilbertCurve.class, HilbertUtil.class});
        sparkConf.setAppName("Trajectory Loading in GeoParquet");
        if (!sparkConf.contains("spark.master")) {
            sparkConf.setMaster("local[*]").set("spark.executor.memory","4g");
        }
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        Broadcast smallHilbertCurveBr = jsc.broadcast(hilbertCurve);
        long startTime = System.currentTimeMillis();

        JavaPairRDD<String, List<Tuple3<Double, Double, Long>>> rdd1 = jsc.textFile(rawDataPath).map(f->f.split(delimiter)).groupBy(f-> f[objectIdIndex])
                .mapToPair(f-> {
                    List<Tuple3<Double, Double, Long>> tuple = new ArrayList<>();
                    for (String[] strings : f._2) {
                        long timestamp = -1;
                        try {
                            timestamp = sdf.parse(strings[timeIndex]).getTime();
                        } catch (Exception e) {
                            continue;
                        }
                        tuple.add(Tuple3.apply(Double.parseDouble(strings[longitudeIndex]), Double.parseDouble(strings[latitudeIndex]), timestamp));
                    }

                    return Tuple2.apply(f._1,tuple);
                }).cache();

        Bounds bounds = rdd1.aggregate(
                new Bounds(),
                (acc, l) -> {
                    acc.add(l._2);return acc;},
                (a, b) -> { a.merge(b); return a; }
        );

        final double minLon = bounds.getMinLongitude();
        final double minLat = bounds.getMinLatitude();
        final long minTime = bounds.getMinTimestamp();
        final double maxLon = bounds.getMaxLongitude()+0.0000001;
        final double maxLat = bounds.getMaxLatitude()+0.0000001;
        final long maxTime = bounds.getMaxTimestamp()+1000;

        if(indexType.equals("3D")) {
            indexUtils = new IndexUtils3D(minLon, minLat, minTime, maxLon, maxLat, maxTime, maxOrdinates);
        }else {
            indexUtils = new IndexUtils2D(minLon, minLat, maxLon, maxLat, maxOrdinates);
        }

        JavaPairRDD rdd = rdd1.flatMapToPair(f->{

            List<Tuple3<Double, Double, Long>> tuple = f._2;

            tuple.sort(Comparator.comparingLong(Tuple3::_3));

                    String objectId = f._1;

            List<Tuple2<Long, Trajectory>> trajectoryParts = new ArrayList<>();
            List<Tuple3<Double, Double, Long>> currentPart = new ArrayList<>();

            //initialize for the currentHilValue
            int part = 1;
            long[] hil = indexUtils.scale(tuple.get(0)._1(), tuple.get(0)._2(), tuple.get(0)._3());//HilbertUtil.scaleGeoTemporalPoint(tuple.get(0)._1(), minLon, maxLon, tuple.get(0)._2(), minLat, maxLat, tuple.get(0)._3(), minTime, maxTime, maxOrdinates);
            Ranges ranges = ((SmallHilbertCurve)smallHilbertCurveBr.getValue()).query(hil, hil, 0);
            long currentHilValue = ranges.toList().get(0).low();
            currentPart.add(Tuple3.apply(tuple.get(0)._1(), tuple.get(0)._2(),tuple.get(0)._3()));

            for (int i = 1; i < tuple.size(); i++) {
                hil = indexUtils.scale(tuple.get(i)._1(), tuple.get(i)._2(), tuple.get(i)._3());//HilbertUtil.scaleGeoTemporalPoint(tuple.get(i)._1(), minLon, maxLon, tuple.get(i)._2(), minLat, maxLat, tuple.get(i)._3(), minTime, maxTime, maxOrdinates);
                ranges = ((SmallHilbertCurve)smallHilbertCurveBr.getValue()).query(hil, hil, 0);
                long hilbertValue = ranges.toList().get(0).low();

                currentPart.add(Tuple3.apply(tuple.get(i)._1(), tuple.get(i)._2(),tuple.get(i)._3()));

                if(currentHilValue != hilbertValue){

                    double minLongitude = Double.MAX_VALUE;
                    double minLatitude = Double.MAX_VALUE;
                    long minTimestamp = Long.MAX_VALUE;

                    double maxLongitude = -Double.MAX_VALUE;
                    double maxLatitude = -Double.MAX_VALUE;
                    long maxTimestamp = Long.MIN_VALUE;

                    for (int j = 0; j < currentPart.size(); j++) {
                        if (Double.compare(minLongitude, currentPart.get(j)._1()) == 1) {
                            minLongitude = currentPart.get(j)._1();
                        }
                        if (Double.compare(minLatitude, currentPart.get(j)._2()) == 1) {
                            minLatitude = currentPart.get(j)._2();
                        }
                        if (Long.compare(minTimestamp, currentPart.get(j)._3()) == 1) {
                            minTimestamp = currentPart.get(j)._3();
                        }
                        if (Double.compare(maxLongitude, currentPart.get(j)._1()) == -1) {
                            maxLongitude = currentPart.get(j)._1();
                        }
                        if (Double.compare(maxLatitude, currentPart.get(j)._2()) == -1) {
                            maxLatitude = currentPart.get(j)._2();
                        }
                        if (Long.compare(maxTimestamp, currentPart.get(j)._3()) == -1) {
                            maxTimestamp = currentPart.get(j)._3();
                        }
                    }


                    GeometryFactory g =new GeometryFactory();
                    Coordinate[] coordinates = new Coordinate[currentPart.size()];
                    long[] timestamps = new long[currentPart.size()];

                    for (int k = 0; k < currentPart.size(); k++) {
                        coordinates[k] = new Coordinate(currentPart.get(k)._1(),currentPart.get(k)._2());
                        timestamps[k] = currentPart.get(k)._3();
                    }

                    trajectoryParts.add(Tuple2.apply(currentHilValue,new Trajectory(objectId, part++, g.createLineString(coordinates), timestamps, minLongitude, minLatitude, minTimestamp, maxLongitude, maxLatitude, maxTimestamp)));

                    currentPart.clear();
                    if(i!=tuple.size()-1) {
                        currentPart.add(Tuple3.apply(tuple.get(i)._1(), tuple.get(i)._2(), tuple.get(i)._3()));
                    }
                    currentHilValue = hilbertValue;
                }
            }

            //leftovers in the currentPartList
            if(currentPart.size()>0){
                double minLongitude = Double.MAX_VALUE;
                double minLatitude = Double.MAX_VALUE;
                long minTimestamp = Long.MAX_VALUE;

                double maxLongitude = -Double.MAX_VALUE;
                double maxLatitude = -Double.MAX_VALUE;
                long maxTimestamp = Long.MIN_VALUE;

                for (int j = 0; j < currentPart.size(); j++) {
                    if (Double.compare(minLongitude, currentPart.get(j)._1()) == 1) {
                        minLongitude = currentPart.get(j)._1();
                    }
                    if (Double.compare(minLatitude, currentPart.get(j)._2()) == 1) {
                        minLatitude = currentPart.get(j)._2();
                    }
                    if (Long.compare(minTimestamp, currentPart.get(j)._3()) == 1) {
                        minTimestamp = currentPart.get(j)._3();
                    }
                    if (Double.compare(maxLongitude, currentPart.get(j)._1()) == -1) {
                        maxLongitude = currentPart.get(j)._1();
                    }
                    if (Double.compare(maxLatitude, currentPart.get(j)._2()) == -1) {
                        maxLatitude = currentPart.get(j)._2();
                    }
                    if (Long.compare(maxTimestamp, currentPart.get(j)._3()) == -1) {
                        maxTimestamp = currentPart.get(j)._3();
                    }
                }

                GeometryFactory g =new GeometryFactory();
                Coordinate[] coordinates = new Coordinate[currentPart.size()];
                long[] timestamps = new long[currentPart.size()];

                for (int k = 0; k < currentPart.size(); k++) {
                    coordinates[k] = new Coordinate(currentPart.get(k)._1(),currentPart.get(k)._2());
                    timestamps[k] = currentPart.get(k)._3();
                }

                trajectoryParts.add(Tuple2.apply(currentHilValue,new Trajectory(objectId, part++, g.createLineString(coordinates), timestamps, minLongitude, minLatitude, minTimestamp, maxLongitude, maxLatitude, maxTimestamp)));

                currentPart.clear();
            }

            return trajectoryParts.iterator();

        }).mapToPair((t)->{return Tuple2.apply(new HilbertKeyTimestamp(t._1, t._2.getMinTimestamp()),t._2);}).sortByKey().mapToPair(f->Tuple2.apply(null, f._2));

        rdd.saveAsNewAPIHadoopFile(writePath, Void.class, Trajectory.class, ParquetOutputFormat.class, job.getConfiguration());

        long endTime = System.currentTimeMillis();
        System.out.println("Exec Time: "+(endTime-startTime));

        try(BufferedWriter bf = new BufferedWriter(new FileWriter(metricsPathExport+ File.separator+"data-loading-geoparquet-"+ Paths.get(writePath).getFileName().toString()+".txt"))) {
            bf.write("Write Time");
            bf.newLine();
            bf.write(String.valueOf((endTime - startTime)/1000));
        }
    }
}
