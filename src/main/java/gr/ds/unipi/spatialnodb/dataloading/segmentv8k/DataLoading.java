package gr.ds.unipi.spatialnodb.dataloading.segmentv8k;

import com.typesafe.config.Config;
import gr.ds.unipi.spatialnodb.AppConfig;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.messages.common.segmentv8.SpatioTemporalPoint;
import gr.ds.unipi.spatialnodb.messages.common.segmentv8.TrajectorySegment;
import gr.ds.unipi.spatialnodb.messages.common.segmentv8.TrajectorySegmentWriteSupport;
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

        final long maxOrdinates = 1L << bits;
        final SmallHilbertCurve hilbertCurve = HilbertCurve.small().bits(bits).dimensions(3);

        Job job = Job.getInstance();

        ParquetOutputFormat.setCompression(job, CompressionCodecName.UNCOMPRESSED);

        ParquetOutputFormat.setWriteSupportClass(job, TrajectorySegmentWriteSupport.class);

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
                    continue;
                }
                tuple.add(Tuple3.apply(Double.parseDouble(strings[longitudeIndex]), Double.parseDouble(strings[latitudeIndex]), timestamp));
            }
            tuple.sort(Comparator.comparingLong(Tuple3::_3));

            String objectId = f._1;

            List<Tuple2<Long, TrajectorySegment>> trajectoryParts = new ArrayList<>();
            List<SpatioTemporalPoint> currentPart = new ArrayList<>();

            //initialize for the currentHilValue
            int part = 1;
            for (int i = 0; i < tuple.size(); i++) {
                //System.out.println(objectId+" "+tuple.get(i)._1() +" "+ minLon+" "+ maxLon+" "+ tuple.get(i)._2()+" "+ minLat+" "+ maxLat+" "+ tuple.get(i)._3()+" "+ minTime+" "+ maxTime+" "+ maxOrdinates);

                currentPart.add(new SpatioTemporalPoint(tuple.get(i)._1(), tuple.get(i)._2(),tuple.get(i)._3()));

                if(currentPart.size()==segK){

                    double minLongitude = Double.MAX_VALUE;
                    double minLatitude = Double.MAX_VALUE;
                    long minTimestamp = Long.MAX_VALUE;

                    double maxLongitude = Double.MIN_VALUE;
                    double maxLatitude = Double.MIN_VALUE;
                    long maxTimestamp = Long.MIN_VALUE;

                    for (int j = 0; j < currentPart.size(); j++) {
                        if (Double.compare(minLongitude, currentPart.get(j).getLongitude()) == 1) {
                            minLongitude = currentPart.get(j).getLongitude();
                        }
                        if (Double.compare(minLatitude, currentPart.get(j).getLatitude()) == 1) {
                            minLatitude = currentPart.get(j).getLatitude();
                        }
                        if (Long.compare(minTimestamp, currentPart.get(j).getTimestamp()) == 1) {
                            minTimestamp = currentPart.get(j).getTimestamp();
                        }
                        if (Double.compare(maxLongitude, currentPart.get(j).getLongitude()) == -1) {
                            maxLongitude = currentPart.get(j).getLongitude();
                        }
                        if (Double.compare(maxLatitude, currentPart.get(j).getLatitude()) == -1) {
                            maxLatitude = currentPart.get(j).getLatitude();
                        }
                        if (Long.compare(maxTimestamp, currentPart.get(j).getTimestamp()) == -1) {
                            maxTimestamp = currentPart.get(j).getTimestamp();
                        }
                    }

                    long[] hil = HilbertUtil.scaleGeoTemporalPoint(currentPart.get(0).getLongitude(), minLon, maxLon, currentPart.get(0).getLatitude(), minLat, maxLat, currentPart.get(0).getTimestamp(), minTime, maxTime, maxOrdinates);
                    Ranges ranges = ((SmallHilbertCurve)smallHilbertCurveBr.getValue()).query(hil, hil, 0);
                    long hilbertValue = ranges.toList().get(0).low();

                    trajectoryParts.add(Tuple2.apply(hilbertValue,new TrajectorySegment(objectId, part++, currentPart.toArray(new SpatioTemporalPoint[0]), minLongitude, minLatitude, minTimestamp, maxLongitude, maxLatitude, maxTimestamp)));

                    currentPart.clear();
                    if(i!=tuple.size()-1) {
                        currentPart.add(new SpatioTemporalPoint(tuple.get(i)._1(), tuple.get(i)._2(), tuple.get(i)._3()));
                    }
                }
            }

            //leftovers in the currentPartList
            if(currentPart.size()>0){
                double minLongitude = Double.MAX_VALUE;
                double minLatitude = Double.MAX_VALUE;
                long minTimestamp = Long.MAX_VALUE;

                double maxLongitude = Double.MIN_VALUE;
                double maxLatitude = Double.MIN_VALUE;
                long maxTimestamp = Long.MIN_VALUE;

                for (int j = 0; j < currentPart.size(); j++) {
                    if (Double.compare(minLongitude, currentPart.get(j).getLongitude()) == 1) {
                        minLongitude = currentPart.get(j).getLongitude();
                    }
                    if (Double.compare(minLatitude, currentPart.get(j).getLatitude()) == 1) {
                        minLatitude = currentPart.get(j).getLatitude();
                    }
                    if (Long.compare(minTimestamp, currentPart.get(j).getTimestamp()) == 1) {
                        minTimestamp = currentPart.get(j).getTimestamp();
                    }
                    if (Double.compare(maxLongitude, currentPart.get(j).getLongitude()) == -1) {
                        maxLongitude = currentPart.get(j).getLongitude();
                    }
                    if (Double.compare(maxLatitude, currentPart.get(j).getLatitude()) == -1) {
                        maxLatitude = currentPart.get(j).getLatitude();
                    }
                    if (Long.compare(maxTimestamp, currentPart.get(j).getTimestamp()) == -1) {
                        maxTimestamp = currentPart.get(j).getTimestamp();
                    }
                }

                long[] hil = HilbertUtil.scaleGeoTemporalPoint(currentPart.get(0).getLongitude(), minLon, maxLon, currentPart.get(0).getLatitude(), minLat, maxLat, currentPart.get(0).getTimestamp(), minTime, maxTime, maxOrdinates);
                Ranges ranges = ((SmallHilbertCurve)smallHilbertCurveBr.getValue()).query(hil, hil, 0);
                long hilbertValue = ranges.toList().get(0).low();

                trajectoryParts.add(Tuple2.apply(hilbertValue,new TrajectorySegment(objectId, part++, currentPart.toArray(new SpatioTemporalPoint[0]), minLongitude, minLatitude, minTimestamp, maxLongitude, maxLatitude, maxTimestamp)));

                currentPart.clear();
            }

            return trajectoryParts.iterator();

        }).sortByKey().mapToPair(f->Tuple2.apply(null, f._2));

        rdd.saveAsNewAPIHadoopFile(writePath, Void.class, TrajectorySegment.class, ParquetOutputFormat.class, job.getConfiguration());

        long endTime = System.currentTimeMillis();
        System.out.println("Exec Time: "+(endTime-startTime));
    }



//    Job job = Job.getInstance();
//
////        ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
//
//        ParquetOutputFormat.setWriteSupportClass(job, TrajectorySegmentWriteSupport .class);
//
//    SimpleDateFormat sdf =  new SimpleDateFormat(dateFormat);
//    SimpleDateFormat sdf1 =  new SimpleDateFormat("yyyy-MM-dd");
//
//    SparkConf sparkConf = new SparkConf().setMaster("local[1]").set("spark.executor.memory","1g").registerKryoClasses(new Class[]{SmallHilbertCurve.class, HilbertUtil.class});
//    SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
//    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
//
//    Broadcast smallHilbertCurveBr = jsc.broadcast(hilbertCurve);
//
//    JavaPairRDD rdd = jsc.textFile(rawDataPath).map(f->f.split(delimiter)).groupBy(f-> f[objectIdIndex]).flatMapToPair(f->{
//        List<Tuple3<Double, Double, Long>> tuple = new ArrayList<>();
//
//        for (String[] strings : f._2) {
//            long timestamp = -1;
//            try {
//                timestamp = sdf.parse(strings[timeIndex]).getTime();
//            }catch (Exception e){
//                continue;
//            }
//            tuple.add(Tuple3.apply(Double.parseDouble(strings[longitudeIndex]), Double.parseDouble(strings[latitudeIndex]), timestamp));
//        }
//        tuple.sort(Comparator.comparingLong(Tuple3::_3));
//
//        String objectId = f._1;
//
//        List<Tuple2<Long, TrajectorySegment>> trajectorySegments = new ArrayList<>();
//
//        int segment = 1;
//        for (int i = 0; i < tuple.size()-1; i++) {
//            long[] hil = HilbertUtil.scaleGeoTemporalPoint(tuple.get(i)._1(), minLon, maxLon, tuple.get(i)._2(), minLat, maxLat, tuple.get(i)._3(), minTime, maxTime, maxOrdinates);
//            Ranges ranges = ((SmallHilbertCurve)smallHilbertCurveBr.getValue()).query(hil, hil, 0);
//            long hilbertValue = ranges.toList().get(0).low();
//
//            Tuple2<Long, TrajectorySegment> seg = Tuple2.apply(hilbertValue, new TrajectorySegment(objectId, segment++, Math.min(tuple.get(i)._1(),tuple.get(i+1)._1()), Math.min(tuple.get(i)._2(),tuple.get(i+1)._2()), tuple.get(i)._3(), Math.max(tuple.get(i)._1(),tuple.get(i+1)._1()), Math.max(tuple.get(i)._2(),tuple.get(i+1)._2()), tuple.get(i+1)._3()));
//            trajectorySegments.add(seg);
//        }
//        return trajectorySegments.iterator();
//    }).sortByKey().mapToPair(f->Tuple2.apply(null, f._2));
//
//        rdd.saveAsNewAPIHadoopFile(writePath, Void.class, TrajectorySegment.class, ParquetOutputFormat.class, job.getConfiguration());

}
