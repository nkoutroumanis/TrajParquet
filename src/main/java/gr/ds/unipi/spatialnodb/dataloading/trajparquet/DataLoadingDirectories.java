package gr.ds.unipi.spatialnodb.dataloading.trajparquet;

import com.typesafe.config.Config;
import gr.ds.unipi.spatialnodb.AppConfig;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.hadoop.MultipleParquetOutputsFormat;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.SpatioTemporalPoint;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.TrajectorySegment;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.TrajectorySegmentWriteSupport;
import gr.ds.unipi.spatialnodb.shapes.STPoint;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.HashPartitioner;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public class DataLoadingDirectories {
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

        JavaPairRDD rdd = jsc.textFile(rawDataPath).map(f->f.split(delimiter)).groupBy(f-> f[objectIdIndex])
                .flatMapToPair(f-> {
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

            Comparator<Tuple3<Double, Double, Long>> comp = Comparator.comparingLong(d-> d._3());
            comp = comp.thenComparingDouble(d-> d._1());
            comp = comp.thenComparingDouble(d-> d._2());
            tuple.sort(comp);

            String objectId = f._1;

            List<Tuple2<Long, TrajectorySegment>> trajectoryParts = new ArrayList<>();
            List<SpatioTemporalPoint> currentPart = new ArrayList<>();

            //initialize for the currentHilValue
            int part = 1;
            long[] hil1 = HilbertUtil.scaleGeoTemporalPoint(tuple.get(0)._1(), minLon, maxLon, tuple.get(0)._2(), minLat, maxLat, tuple.get(0)._3(), minTime, maxTime, maxOrdinates);
            Ranges ranges = ((SmallHilbertCurve)smallHilbertCurveBr.getValue()).query(hil1, hil1, 0);
            long currentHilValue = ranges.toList().get(0).low();
            currentPart.add(new SpatioTemporalPoint(tuple.get(0)._1(), tuple.get(0)._2(),tuple.get(0)._3()));

            for (int i = 1; i < tuple.size(); i++) {
                long[] hil2 = HilbertUtil.scaleGeoTemporalPoint(tuple.get(i)._1(), minLon, maxLon, tuple.get(i)._2(), minLat, maxLat, tuple.get(i)._3(), minTime, maxTime, maxOrdinates);
                ranges = ((SmallHilbertCurve)smallHilbertCurveBr.getValue()).query(hil2, hil2, 0);
                long hilbertValue = ranges.toList().get(0).low();

                if(currentHilValue != hilbertValue){

                    SpatioTemporalPoint pointLast = null;
                    SpatioTemporalPoint pointBegin = null;

                    List<Tuple2<Long,SpatioTemporalPoint[]>> passingSegments = new ArrayList<>();

                    ranges = ((SmallHilbertCurve)smallHilbertCurveBr.getValue()).query(hil1, hil2, 0);

                    List<Range> rangesList = ranges.toList();

                    if(!(rangesList.size()==1 && (rangesList.get(0).low() == rangesList.get(0).high()))){
                        for (Range range : rangesList) {
                            for (long cubeIndex = range.low(); cubeIndex<=range.high();cubeIndex++){
                                long[] cube =  ((SmallHilbertCurve)smallHilbertCurveBr.getValue()).point(cubeIndex);

                                double xMin = minLon + (cube[0] * (maxLon-minLon)/(maxOrdinates+ 1L));
                                double yMin = minLat + (cube[1] * (maxLat-minLat)/(maxOrdinates+ 1L));
                                long tMin = minTime + (cube[2] * (maxTime-minTime)/(maxOrdinates+ 1L));

                                double xMax = minLon + ((cube[0]+1) * (maxLon-minLon)/(maxOrdinates+ 1L));
                                double yMax = minLat + ((cube[1]+1) * (maxLat-minLat)/(maxOrdinates+ 1L));
                                long tMax = minTime + ((cube[2]+1) * (maxTime-minTime)/(maxOrdinates+ 1L));

                                Optional<STPoint[]> stPoints = HilbertUtil.liangBarsky(tuple.get(i-1)._1(), tuple.get(i-1)._2(), tuple.get(i-1)._3(), tuple.get(i)._1(), tuple.get(i)._2(), tuple.get(i)._3(), xMin, yMin, tMin, xMax, yMax, tMax );

                                List<Tuple3<Double, Double, Long>> newPoints = new ArrayList<>();
                                if(stPoints.isPresent()){

                                    if(Double.compare(stPoints.get()[0].getX(), tuple.get(i-1)._1())!=0 || Double.compare(stPoints.get()[0].getY(), tuple.get(i-1)._2())!=0 || stPoints.get()[0].getT() != tuple.get(i - 1)._3()){
                                        newPoints.add(new Tuple3<>(stPoints.get()[0].getX(), stPoints.get()[0].getY(), stPoints.get()[0].getT()));
                                    }

                                    if(Double.compare(stPoints.get()[1].getX(),tuple.get(i)._1())!=0 || Double.compare(stPoints.get()[1].getY(), tuple.get(i)._2())!=0 || stPoints.get()[1].getT() != tuple.get(i)._3()){
                                        newPoints.add(new Tuple3<>(stPoints.get()[1].getX(), stPoints.get()[1].getY(), stPoints.get()[1].getT()));
                                    }

                                    if(newPoints.size()==2){
                                        SpatioTemporalPoint[] stps = new SpatioTemporalPoint[2];
                                        stps[0] = new SpatioTemporalPoint(newPoints.get(0)._1(), newPoints.get(0)._2(),newPoints.get(0)._3());
                                        stps[1] = new SpatioTemporalPoint(newPoints.get(1)._1(), newPoints.get(1)._2(),newPoints.get(1)._3());

                                        if(cubeIndex==currentHilValue){
                                            pointLast = new SpatioTemporalPoint(newPoints.get(1)._1(), newPoints.get(1)._2(), newPoints.get(1)._3());
                                        }else if(cubeIndex == hilbertValue){
                                            pointBegin = new SpatioTemporalPoint(newPoints.get(0)._1(), newPoints.get(0)._2(), newPoints.get(0)._3());
                                        }else{
                                            passingSegments.add(Tuple2.apply(cubeIndex, stps));
                                        }

                                    }else if(newPoints.size()==1){
                                        if(cubeIndex == currentHilValue){
                                            pointLast = new SpatioTemporalPoint(newPoints.get(0)._1(), newPoints.get(0)._2(), newPoints.get(0)._3());
                                        }else if(cubeIndex == hilbertValue){
                                            pointBegin = new SpatioTemporalPoint(newPoints.get(0)._1(), newPoints.get(0)._2(), newPoints.get(0)._3());
                                        }
                                        else{
                                            throw new Exception("The array from the Liang Barsky should contain at least one element");
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if(pointLast!=null){
                        currentPart.add(pointLast);
                    }else{
                        throw new Exception("Point last should exist, but it is "+pointLast);
                    }

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

                    trajectoryParts.add(Tuple2.apply(currentHilValue, new TrajectorySegment(objectId, part++, currentPart.toArray(new SpatioTemporalPoint[0]), minLongitude, minLatitude, minTimestamp, maxLongitude, maxLatitude, maxTimestamp)));
                    currentPart.clear();

                    Comparator<Tuple2<Long, SpatioTemporalPoint[]>> comparator = Comparator.comparingLong(d-> d._2[0].getTimestamp());
                    //the second comparator is not really needed, but it can handle the intersected points of lines with cubes that have the same timestamp.
                    comparator = comparator.thenComparingLong(d-> d._2[1].getTimestamp());
                    //the third comparator is not really needed, but it can handle erroneous data sets in terms of containing more than one points of an object id with the same timestamp but with different location
                    if(Double.compare(pointLast.getLongitude(),pointBegin.getLongitude())==-1){
                        comparator = comparator.thenComparingDouble(d->d._2[0].getLongitude());
                    }else if(Double.compare(pointLast.getLongitude(),pointBegin.getLongitude())==1){
                        comparator = comparator.thenComparingDouble(d->d._2[0].getLongitude()*(-1));
                    }
                    if (Double.compare(pointLast.getLatitude(),pointBegin.getLatitude())==-1) {
                        comparator = comparator.thenComparingDouble(d->d._2[0].getLatitude());
                    }else if(Double.compare(pointLast.getLatitude(),pointBegin.getLatitude())==1){
                        comparator = comparator.thenComparingDouble(d->d._2[0].getLatitude()*(-1));
                    }
                    passingSegments.sort(comparator);

                    for (Tuple2<Long, SpatioTemporalPoint[]> passingSegment : passingSegments) {
                        trajectoryParts.add(Tuple2.apply(passingSegment._1, new TrajectorySegment(objectId, part++, passingSegment._2, Math.min(passingSegment._2[0].getLongitude(), passingSegment._2[1].getLongitude()), Math.min(passingSegment._2[0].getLatitude(), passingSegment._2[1].getLatitude()), passingSegment._2[0].getTimestamp(), Math.max(passingSegment._2[0].getLongitude(), passingSegment._2[1].getLongitude()), Math.max(passingSegment._2[0].getLatitude(), passingSegment._2[1].getLatitude()), passingSegment._2[1].getTimestamp())));
                    }

//                    if(passingSegments.size()>0) {
//                        if (passingSegments.get(passingSegments.size() - 1)._2[passingSegments.get(passingSegments.size() - 1)._2.length - 1].getTimestamp() != pointBegin.getTimestamp()) {
//                            System.out.println(passingSegments.get(passingSegments.size() - 1)._2[passingSegments.get(passingSegments.size() - 1)._2.length - 1].getTimestamp() + " "+ pointBegin.getTimestamp());
//                            throw new Exception("HERE EXCEptION "+f._1 +" passingsegs"+passingSegments.get(passingSegments.size()-1)._2.length);
//                        }
//                    }

                    for (int i1 = 1; i1 < passingSegments.size(); i1++) {
                        if(passingSegments.get(i1-1)._2[1].getTimestamp() != passingSegments.get(i1)._2[0].getTimestamp() || passingSegments.get(i1-1)._2[1].getLongitude() != passingSegments.get(i1)._2[0].getLongitude() || passingSegments.get(i1-1)._2[1].getLatitude() != passingSegments.get(i1)._2[0].getLatitude()){
                            passingSegments.forEach(pair-> System.out.println(pair._2[0] + "-"+pair._2[1]));
                            throw new Exception("Problem with the passing segments list. A point seems not to be the same with the first point in the next segment. Object id: "+f._1+" Points: "+passingSegments.get(i1-1)._2[1] + " "+passingSegments.get(i1)._2[0]);
                        }
                    }


                    passingSegments.clear();

                    if(pointBegin!=null) {
                        currentPart.add(pointBegin);
                    }else{
                        throw new Exception("Point last should exist, but it is "+pointBegin);
                    }

                    currentPart.add(new SpatioTemporalPoint(tuple.get(i)._1(), tuple.get(i)._2(), tuple.get(i)._3()));

                    currentHilValue = hilbertValue;
                    hil1 = hil2;
                }else{
                    currentPart.add(new SpatioTemporalPoint(tuple.get(i)._1(), tuple.get(i)._2(),tuple.get(i)._3()));
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

                if(currentPart.size()==1){
                    throw new Exception("There is a trajectory segment containing one point");
                }
                trajectoryParts.add(Tuple2.apply(currentHilValue,new TrajectorySegment(objectId, part++, currentPart.toArray(new SpatioTemporalPoint[0]), minLongitude, minLatitude, minTimestamp, maxLongitude, maxLatitude, maxTimestamp)));

                currentPart.clear();
            }

            trajectoryParts.sort(Comparator.comparingLong(d-> d._2.getSpatioTemporalPoints()[0].getTimestamp()));
            for (int i = 0; i < trajectoryParts.size()-1; i++) {
                if(trajectoryParts.get(i)._2.getSpatioTemporalPoints()[trajectoryParts.get(i)._2.getSpatioTemporalPoints().length-1].getTimestamp() != trajectoryParts.get(i+1)._2.getSpatioTemporalPoints()[0].getTimestamp() ||
                        trajectoryParts.get(i)._2.getSpatioTemporalPoints()[trajectoryParts.get(i)._2.getSpatioTemporalPoints().length-1].getLongitude() != trajectoryParts.get(i+1)._2.getSpatioTemporalPoints()[0].getLongitude() ||
                        trajectoryParts.get(i)._2.getSpatioTemporalPoints()[trajectoryParts.get(i)._2.getSpatioTemporalPoints().length-1].getLatitude() != trajectoryParts.get(i+1)._2.getSpatioTemporalPoints()[0].getLatitude())
                {
                    System.out.println(trajectoryParts.get(i)._2.getSegment()+" - "+trajectoryParts.get(i)._2.getSpatioTemporalPoints().length+" "+trajectoryParts.get(i+1)._2.getSpatioTemporalPoints().length+" "+trajectoryParts.get(i)._2.getSpatioTemporalPoints()[trajectoryParts.get(i)._2.getSpatioTemporalPoints().length-1].getTimestamp() +" "+ trajectoryParts.get(i+1)._2.getSpatioTemporalPoints()[0].getTimestamp() +" "+
                            trajectoryParts.get(i)._2.getSpatioTemporalPoints()[trajectoryParts.get(i)._2.getSpatioTemporalPoints().length-1].getLongitude() +" "+ trajectoryParts.get(i+1)._2.getSpatioTemporalPoints()[0].getLongitude() +" "+
                            trajectoryParts.get(i)._2.getSpatioTemporalPoints()[trajectoryParts.get(i)._2.getSpatioTemporalPoints().length-1].getLatitude() +" "+ trajectoryParts.get(i+1)._2.getSpatioTemporalPoints()[0].getLatitude());
                    throw new Exception("Problem concerning the linking of the points in the trajectory segments. Object id: "+f._1+" Total trajectory segments "+trajectoryParts.size());
                }
            }

            for (int i = 0; i < trajectoryParts.size()-1; i++) {
                if(trajectoryParts.get(i)._2.getSegment() + 1 != trajectoryParts.get(i+1)._2.getSegment()){
                    throw new Exception("Problem concerning the numbering of trajectory segments.");
                }
            }

            Tuple2<Long,TrajectorySegment> trjSeg = trajectoryParts.get(trajectoryParts.size()-1);
            Tuple2<Long,TrajectorySegment> newTrjSeg = new Tuple2<>(trjSeg._1, new TrajectorySegment(trjSeg._2.getObjectId(), -1* trjSeg._2.getSegment(), trjSeg._2.getSpatioTemporalPoints(), trjSeg._2.getMinLongitude(), trjSeg._2.getMinLatitude(), trjSeg._2.getMinTimestamp(), trjSeg._2.getMaxLongitude(), trjSeg._2.getMaxLatitude(), trjSeg._2.getMaxTimestamp()));
            trajectoryParts.set(trajectoryParts.size()-1, newTrjSeg);

            return trajectoryParts.iterator();

        })
                .repartitionAndSortWithinPartitions(new HashPartitioner(1000)).mapToPair(f->Tuple2.apply(Tuple2.apply(f._1+"/", null), f._2));
        rdd.saveAsNewAPIHadoopFile(writePath, Void.class, TrajectorySegment.class, MultipleParquetOutputsFormat.class, job.getConfiguration());

        long endTime = System.currentTimeMillis();
        System.out.println("Exec Time: "+(endTime-startTime));

    }

}
