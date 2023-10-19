package gr.ds.unipi.spatialnodb.dataloading.trajparquet;

import com.typesafe.config.Config;
import gr.ds.unipi.spatialnodb.AppConfig;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.SpatioTemporalPoint;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.TrajectorySegment;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.TrajectorySegmentWriteSupport;
import gr.ds.unipi.spatialnodb.shapes.STPoint;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.Range;
import org.davidmoten.hilbert.Ranges;
import org.davidmoten.hilbert.SmallHilbertCurve;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DataLoading {
    public static void main(String[] args) throws IOException {

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

        final int bits = hilbert.getInt("bits");
        final double minLon = hilbert.getDouble("minLon");
        final double minLat = hilbert.getDouble("minLat");
        final long minTime = hilbert.getLong("minTime");
        final double maxLon = hilbert.getDouble("maxLon");
        final double maxLat = hilbert.getDouble("maxLat");
        final long maxTime = hilbert.getLong("maxTime");

        final SmallHilbertCurve hilbertCurve = HilbertCurve.small().bits(bits).dimensions(3);
        final long maxOrdinates = hilbertCurve.maxOrdinate();

        Job job = Job.getInstance();

        ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);

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

//            double lonLength = (maxLon-minLon)/(maxOrdinates+1l);
//            double latLength = (maxLat-minLat)/(maxOrdinates+1l);
//            long timeLength = (maxTime-minTime)/(maxOrdinates+1L);

            LinkedHashSet<Tuple3<Double, Double, Long>> newPoints = new LinkedHashSet<>();
            for (int i = 1; i < tuple.size(); i++) {
                long[] hilStart = HilbertUtil.scaleGeoTemporalPoint(tuple.get(i-1)._1(), minLon, maxLon, tuple.get(i-1)._2(), minLat, maxLat, tuple.get(i-1)._3(), minTime, maxTime, maxOrdinates);
                long[] hilEnd = HilbertUtil.scaleGeoTemporalPoint(tuple.get(i)._1(), minLon, maxLon, tuple.get(i)._2(), minLat, maxLat, tuple.get(i)._3(), minTime, maxTime, maxOrdinates);

                Ranges ranges = ((SmallHilbertCurve)smallHilbertCurveBr.getValue()).query(hilStart, hilEnd, 0);

                List<Range> rangesList = ranges.toList();
                if(!(rangesList.size()==1 && (rangesList.get(0).low() == rangesList.get(0).high()))){
                    for (Range range : rangesList) {
                        for (long cubeIndex = range.low(); cubeIndex<=range.high();cubeIndex++){
                            long[] cube =  ((SmallHilbertCurve)smallHilbertCurveBr.getValue()).point(cubeIndex);

                            double xMin = minLon + (cube[0] * (maxLon-minLon)/(maxOrdinates+ 1L));
                            double yMin = minLat + (cube[1] * (maxLat-minLat)/(maxOrdinates+ 1L));
                            long tMin = minTime + (cube[2] * (maxTime-minTime)/(maxOrdinates+ 1L));

//                            double xMax = xMin + lonLength;
//                            double yMax = yMin + latLength;
//                            long tMax = tMin + timeLength;
                            double xMax = minLon + ((cube[0]+1) * (maxLon-minLon)/(maxOrdinates+ 1L));
                            double yMax = minLat + ((cube[1]+1) * (maxLat-minLat)/(maxOrdinates+ 1L));
                            long tMax = minTime + ((cube[2]+1) * (maxTime-minTime)/(maxOrdinates+ 1L));

                            Optional<STPoint[]> stPoints = HilbertUtil.liangBarsky(tuple.get(i-1)._1(), tuple.get(i-1)._2(), tuple.get(i-1)._3(), tuple.get(i)._1(), tuple.get(i)._2(), tuple.get(i)._3(), xMin, yMin, tMin, xMax, yMax, tMax );
                            if(stPoints.isPresent()){
                                if(stPoints.get()[0].getT() != tuple.get(i-1)._3()){
                                    newPoints.add(new Tuple3<>(stPoints.get()[0].getX(), stPoints.get()[0].getY(), stPoints.get()[0].getT()));
                                }
                                if(stPoints.get()[1].getT() != tuple.get(i)._3()){
                                    newPoints.add(new Tuple3<>(stPoints.get()[1].getX(), stPoints.get()[1].getY(), stPoints.get()[1].getT()));
                                }

//                                for (STPoint stPoint : stPoints.get()) {
//
//
//                                    for (Tuple3<Double, Double, Long> newPoint : newPoints) {
//                                        if(newPoint._3() == stPoint.getT() && newPoint._2() != stPoint.getY()&& newPoint._1() != stPoint.getX()){
//                                            throw new Exception("YES THERE EXIST ENTRY WITH THE SAME TIME! time is "+stPoint.getT() +" for object "+f._1);
//                                        }
//                                    }
//
////                                    if(stPoint.getT()==1443705438536l){
////                                        System.out.println("objectId:"+ f._1+" i is"+ i+" cube is"+ cubeIndex+" low:"+range.low()+" high:"+range.high());
////                                        System.out.println("xMin: "+xMin +", yMin: "+yMin+", tMin: " +tMin+" - xMax: "+xMax +", yMax: "+yMax+", tMax:" +tMax);
////                                        System.out.println("tuple.get(i-1)._1(): "+tuple.get(i-1)._1() +", tuple.get(i-1)._2(): "+tuple.get(i-1)._2()+", tuple.get(i-1)._3(): " +tuple.get(i-1)._3() +" - "+"tuple.get(i)._1(): "+tuple.get(i)._1() +", tuple.get(i)._2(): "+tuple.get(i)._2()+", tuple.get(i)._3(): " +tuple.get(i)._3());
////                                        System.out.println(stPoint);
////                                        System.out.println("Point belongs in cube "+((SmallHilbertCurve)smallHilbertCurveBr.getValue()).index(HilbertUtil.scaleGeoTemporalPoint(stPoint.getX(), minLon, maxLon, stPoint.getY(), minLat, maxLat, stPoint.getT(), minTime, maxTime, maxOrdinates)));
////                                    }
//
////                                    if(!newPoints.contains(new Tuple3<>(stPoint.getX(), stPoint.getY(), stPoint.getT()))){
////
////                                        for (Tuple3<Double, Double, Long> newPoint : newPoints) {
////                                        if(newPoint._3() == stPoint.getT()){
////                                            throw new Exception("YES THERE EXIST ENTRY WITH THE SAME TIME! time is "+stPoint.getT() +" for object "+f._1);
////                                        }
////                                    }
////
////                                        newPoints.add(new Tuple3<>(stPoint.getX(), stPoint.getY(), stPoint.getT()));
////                                    }
//                                    newPoints.add(new Tuple3<>(stPoint.getX(), stPoint.getY(), stPoint.getT()));
//
//
//
////                                   if(Long.compare(stPoint.getT(), tuple.get(i)._3()) ==1 ||  Long.compare(stPoint.getT(), tuple.get(i-1)._3()) ==-1 ||
////                                           Long.compare(stPoint.getT(), tMax) ==1 ||  Long.compare(stPoint.getT(), tMin) ==-1
////                                   || stPoint.getT() == tuple.get(i)._3() || stPoint.getT() == tuple.get(i - 1)._3() ||
////                                           stPoint.getT() == tMax || stPoint.getT() == tMin
////                                   ){
////                                       System.out.println("xMin: "+xMin +", yMin: "+yMin+", tMin: " +tMin+" - xMax: "+xMax +", yMax: "+yMax+", tMax:" +tMax);
////                                       System.out.println("tuple.get(i-1)._1(): "+tuple.get(i-1)._1() +", tuple.get(i-1)._2(): "+tuple.get(i-1)._2()+", tuple.get(i-1)._3(): " +tuple.get(i-1)._3() +" - "+"tuple.get(i)._1(): "+tuple.get(i)._1() +", tuple.get(i)._2(): "+tuple.get(i)._2()+", tuple.get(i)._3(): " +tuple.get(i)._3());
////                                       System.out.println(stPoint);
////                                       throw new Exception("EXCEPTION! "+f._1);
////                                   }
//                                }
                            }
                        }
                    }
                }
            }

//            tuple.forEach(System.out::println);
//            System.out.println("Starting new");
//            newPoints.forEach(System.out::println);
//            System.out.println("Had:" + tuple.size()+ ", the addional ones are are: "+newPoints.size());

            tuple.addAll(newPoints);
            tuple.sort(Comparator.comparingLong(Tuple3::_3));

//            if(f._1.equals("250002173")){
//                for (Tuple3<Double, Double, Long> doubleDoubleLongTuple3 : tuple) {
//                    System.out.println(doubleDoubleLongTuple3);
//                }
//            }

            String objectId = f._1;

            List<Tuple2<Long, TrajectorySegment>> trajectoryParts = new ArrayList<>();
            List<SpatioTemporalPoint> currentPart = new ArrayList<>();

            //initialize for the currentHilValue
            int part = 1;
            long[] hil = HilbertUtil.scaleGeoTemporalPoint(tuple.get(0)._1(), minLon, maxLon, tuple.get(0)._2(), minLat, maxLat, tuple.get(0)._3(), minTime, maxTime, maxOrdinates);
            Ranges ranges = ((SmallHilbertCurve)smallHilbertCurveBr.getValue()).query(hil, hil, 0);
            long currentHilValue = ranges.toList().get(0).low();
            currentPart.add(new SpatioTemporalPoint(tuple.get(0)._1(), tuple.get(0)._2(),tuple.get(0)._3()));

            for (int i = 1; i < tuple.size(); i++) {
//                System.out.println(objectId+" "+tuple.get(i)._1() +" "+ minLon+" "+ maxLon+" "+ tuple.get(i)._2()+" "+ minLat+" "+ maxLat+" "+ tuple.get(i)._3()+" "+ minTime+" "+ maxTime+" "+ maxOrdinates);

                hil = HilbertUtil.scaleGeoTemporalPoint(tuple.get(i)._1(), minLon, maxLon, tuple.get(i)._2(), minLat, maxLat, tuple.get(i)._3(), minTime, maxTime, maxOrdinates);
                ranges = ((SmallHilbertCurve)smallHilbertCurveBr.getValue()).query(hil, hil, 0);
                long hilbertValue = ranges.toList().get(0).low();

                currentPart.add(new SpatioTemporalPoint(tuple.get(i)._1(), tuple.get(i)._2(),tuple.get(i)._3()));

                if(currentHilValue != hilbertValue){

                    double minLongitude = Double.MAX_VALUE;
                    double minLatitude = Double.MAX_VALUE;
                    long minTimestamp = Long.MAX_VALUE;

                    double maxLongitude = -Double.MAX_VALUE;
                    double maxLatitude = -Double.MAX_VALUE;
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

                    trajectoryParts.add(Tuple2.apply(currentHilValue,new TrajectorySegment(objectId, part++, currentPart.toArray(new SpatioTemporalPoint[0]), minLongitude, minLatitude, minTimestamp, maxLongitude, maxLatitude, maxTimestamp)));

                    currentPart.clear();
                    if(i!=tuple.size()-1) {
                        currentPart.add(new SpatioTemporalPoint(tuple.get(i)._1(), tuple.get(i)._2(), tuple.get(i)._3()));
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

                trajectoryParts.add(Tuple2.apply(currentHilValue,new TrajectorySegment(objectId, part++, currentPart.toArray(new SpatioTemporalPoint[0]), minLongitude, minLatitude, minTimestamp, maxLongitude, maxLatitude, maxTimestamp)));

                currentPart.clear();
            }

            return trajectoryParts.iterator();

        }).sortByKey().mapToPair(f->Tuple2.apply(null, f._2));

        rdd.saveAsNewAPIHadoopFile(writePath, Void.class, TrajectorySegment.class, ParquetOutputFormat.class, job.getConfiguration());

        long endTime = System.currentTimeMillis();
        System.out.println("Exec Time: "+(endTime-startTime));

    }

}
