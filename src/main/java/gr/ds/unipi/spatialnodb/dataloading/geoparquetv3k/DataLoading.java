package gr.ds.unipi.spatialnodb.dataloading.geoparquetv3k;

import com.typesafe.config.Config;
import gr.ds.unipi.spatialnodb.AppConfig;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.messages.common.geoparquetv3.Trajectory;
import gr.ds.unipi.spatialnodb.messages.common.geoparquetv3.TrajectoryWriteSupport;
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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

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
        final int segK = dataLoading.getInt("segK");

        Config hilbert = dataLoading.getConfig("hilbert");

        final int bits = hilbert.getInt("bits");;
        final double minLon = hilbert.getDouble("minLon");
        final double minLat = hilbert.getDouble("minLat");
        final long minTime = hilbert.getLong("minTime");
        final double maxLon = hilbert.getDouble("maxLon");
        final double maxLat = hilbert.getDouble("maxLat");
        final long maxTime = hilbert.getLong("maxTime");

        final SmallHilbertCurve hilbertCurve = HilbertCurve.small().bits(bits).dimensions(3);
        final long maxOrdinates = hilbertCurve.maxOrdinate();

        Job job = Job.getInstance();

        ParquetOutputFormat.setCompression(job, CompressionCodecName.UNCOMPRESSED);

        ParquetOutputFormat.setWriteSupportClass(job, TrajectoryWriteSupport.class);

        SimpleDateFormat sdf =  new SimpleDateFormat(dateFormat);
        SparkConf sparkConf = new SparkConf()/*.setMaster("local[1]").set("spark.executor.memory","1g")*/.registerKryoClasses(new Class[]{SmallHilbertCurve.class, HilbertUtil.class});
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        Broadcast smallHilbertCurveBr = jsc.broadcast(hilbertCurve);
        long startTime = System.currentTimeMillis();

        JavaPairRDD rdd = jsc.textFile(rawDataPath).map(f->f.split(delimiter)).groupBy(f-> f[objectIdIndex]).flatMapToPair(f-> {
                    List<Tuple3<Double, Double, Long>> tuple = new ArrayList<>();
                    for (String[] strings : f._2) {
                        long timestamp = -1;
                        try {
                            timestamp = sdf.parse(strings[timeIndex]).getTime();
                        }catch (Exception e){
                            System.out.println("EXCEPTION: "+strings[timeIndex]);
                            continue;
                        }
                        tuple.add(Tuple3.apply(Double.parseDouble(strings[longitudeIndex]), Double.parseDouble(strings[latitudeIndex]), timestamp));
                    }
                    tuple.sort(Comparator.comparingLong(Tuple3::_3));


                    String objectId = f._1;

                    List<Tuple2<Long, Trajectory>> trajectoryParts = new ArrayList<>();
                    List<Tuple3<Double, Double, Long>> currentPart = new ArrayList<>();
                    //initialize for the currentHilValue
                    int part = 1;

                    for (int i = 0; i < tuple.size(); i++) {
                        currentPart.add(tuple.get(i));
                        if(currentPart.size()==segK) {
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

                            long[] hil = HilbertUtil.scaleGeoTemporalPoint(currentPart.get(0)._1(), minLon, maxLon, currentPart.get(0)._2(), minLat, maxLat, currentPart.get(0)._3(), minTime, maxTime, maxOrdinates);
                            Ranges ranges = ((SmallHilbertCurve)smallHilbertCurveBr.getValue()).query(hil, hil, 0);
                            long hilbertValue = ranges.toList().get(0).low();


                            GeometryFactory g =new GeometryFactory();
                            Coordinate[] coordinates = new Coordinate[currentPart.size()];
                            long[] timestamps = new long[currentPart.size()];

                            for (int k = 0; k < currentPart.size(); k++) {
                                coordinates[k] = new Coordinate(currentPart.get(k)._1(),currentPart.get(k)._2());
                                timestamps[k] = currentPart.get(k)._3();
                            }

                            trajectoryParts.add(Tuple2.apply(hilbertValue,new Trajectory(objectId, part++, g.createLineString(coordinates), timestamps, minLongitude, minLatitude, minTimestamp, maxLongitude, maxLatitude, maxTimestamp)));

                            currentPart.clear();
                            if(i!=tuple.size()-1) {
                                currentPart.add(Tuple3.apply(tuple.get(i)._1(), tuple.get(i)._2(), tuple.get(i)._3()));
                            }
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

                long[] hil = HilbertUtil.scaleGeoTemporalPoint(currentPart.get(0)._1(), minLon, maxLon, currentPart.get(0)._2(), minLat, maxLat, currentPart.get(0)._3(), minTime, maxTime, maxOrdinates);
                Ranges ranges = ((SmallHilbertCurve)smallHilbertCurveBr.getValue()).query(hil, hil, 0);
                long hilbertValue = ranges.toList().get(0).low();

                GeometryFactory g =new GeometryFactory();
                Coordinate[] coordinates = new Coordinate[currentPart.size()];
                long[] timestamps = new long[currentPart.size()];

                for (int k = 0; k < currentPart.size(); k++) {
                    coordinates[k] = new Coordinate(currentPart.get(k)._1(),currentPart.get(k)._2());
                    timestamps[k] = currentPart.get(k)._3();
                }

                trajectoryParts.add(Tuple2.apply(hilbertValue,new Trajectory(objectId, part++, g.createLineString(coordinates), timestamps,  minLongitude, minLatitude, minTimestamp, maxLongitude, maxLatitude, maxTimestamp)));

                currentPart.clear();
            }
            return trajectoryParts.iterator();

        }).sortByKey().mapToPair(f->Tuple2.apply(null, f._2));

        rdd.saveAsNewAPIHadoopFile(writePath, Void.class, Trajectory.class, ParquetOutputFormat.class, job.getConfiguration());

        long endTime = System.currentTimeMillis();
        System.out.println("Exec Time: "+(endTime-startTime));

    }
}
