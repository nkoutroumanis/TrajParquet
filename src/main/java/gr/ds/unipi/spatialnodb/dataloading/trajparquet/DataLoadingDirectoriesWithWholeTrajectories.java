package gr.ds.unipi.spatialnodb.dataloading.trajparquet;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValueFactory;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.hadoop.MultipleParquetOutputsFormat;
import gr.ds.unipi.spatialnodb.messages.common.*;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.*;
import gr.ds.unipi.spatialnodb.shapes.STPoint;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.Range;
import org.davidmoten.hilbert.Ranges;
import org.davidmoten.hilbert.SmallHilbertCurve;
import scala.Tuple2;
import scala.Tuple3;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;

import static gr.ds.unipi.spatialnodb.AppConfig.loadConfig;

public class DataLoadingDirectoriesWithWholeTrajectories {
    public static void main(String[] args) throws IOException {

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

        final int bits = hilbert.getInt("bits");

        final SmallHilbertCurve hilbertCurve = HilbertCurve.small().bits(bits).dimensions(indexType.equals("3D")?3:2);
        final long maxOrdinates = hilbertCurve.maxOrdinate();

        Job job = Job.getInstance();

        ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        ParquetOutputFormat.setWriteSupportClass(job, TrajectorySegmentWriteSupport.class);

        SimpleDateFormat sdf =  new SimpleDateFormat(dateFormat);
        SparkConf sparkConf = new SparkConf().registerKryoClasses(new Class[]{SmallHilbertCurve.class, HilbertUtil.class});

        sparkConf.setAppName("Trajectory Loading in TrajParquet");
        if (!sparkConf.contains("spark.master")) {
            sparkConf.setMaster("local[*]").set("spark.executor.memory","4g");
        }

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        Broadcast smallHilbertCurveBr = jsc.broadcast(hilbertCurve);
        long startTime = System.currentTimeMillis();

        JavaRDD<TrajectorySegment> trajectoriesRDD = jsc.textFile(rawDataPath).map(f->f.split(delimiter)).groupBy(f-> f[objectIdIndex], Integer.parseInt(args[0])).map(f->{

            String objectId = ((Tuple2<String, Iterable<String[]>>) f)._1;
            int counter = 0;
            Iterator<String[]> it = ((Tuple2<String, Iterable<String[]>>) f)._2.iterator();
            while(it.hasNext()) {
                counter++;
                it.next();
            }

            SpatioTemporalPoint[] spts = new SpatioTemporalPoint[counter];
            counter = 0;
            it = ((Tuple2<String, Iterable<String[]>>) f)._2.iterator();
            while(it.hasNext()) {
                String[] strings = it.next();
                long timestamp = -1;
                try {
                    timestamp = sdf.parse(strings[timeIndex]).getTime();
                }catch (Exception e){
                    e.printStackTrace();
                }
                spts[counter++] = new SpatioTemporalPoint(Double.parseDouble(strings[longitudeIndex]), Double.parseDouble(strings[latitudeIndex]), timestamp);
            }

            Comparator<SpatioTemporalPoint> comp = Comparator.comparingLong(d-> d.getTimestamp());
            comp = comp.thenComparingDouble(d-> d.getLongitude());
            comp = comp.thenComparingDouble(d-> d.getLatitude());
            Arrays.sort(spts, comp);

            double minLongitude = Double.MAX_VALUE;
            double minLatitude = Double.MAX_VALUE;
            long minTimestamp = Long.MAX_VALUE;

            double maxLongitude = -Double.MAX_VALUE;
            double maxLatitude = -Double.MAX_VALUE;
            long maxTimestamp = Long.MIN_VALUE;

            for(int j=0; j < spts.length; j++) {
                if (Double.compare(minLongitude, spts[j].getLongitude()) == 1) {
                    minLongitude = spts[j].getLongitude();
                }
                if (Double.compare(minLatitude, spts[j].getLatitude()) == 1) {
                    minLatitude =spts[j].getLatitude();
                }
                if (Long.compare(minTimestamp, spts[j].getTimestamp()) == 1) {
                    minTimestamp = spts[j].getTimestamp();
                }
                if (Double.compare(maxLongitude, spts[j].getLongitude()) == -1) {
                    maxLongitude = spts[j].getLongitude();
                }
                if (Double.compare(maxLatitude, spts[j].getLatitude()) == -1) {
                    maxLatitude = spts[j].getLatitude();
                }
                if (Long.compare(maxTimestamp, spts[j].getTimestamp()) == -1) {
                    maxTimestamp = spts[j].getTimestamp();
                }
            }
            return new TrajectorySegment(objectId, 0, spts, minLongitude, minLatitude, minTimestamp, maxLongitude, maxLatitude, maxTimestamp);
        }).cache();

        trajectoriesRDD.mapToPair(f-> Tuple2.apply(f.getObjectId(), f)).sortByKey().mapToPair(f->Tuple2.apply(null, f._2)).saveAsNewAPIHadoopFile(writePath+File.separator+"idIndex", Void.class, TrajectorySegment.class, ParquetOutputFormat.class, job.getConfiguration());

        Bounds bounds = trajectoriesRDD.aggregate(
                        new Bounds(),
                        (acc, ts) -> { acc.add(ts); return acc; },
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

        ParquetOutputFormat.setWriteSupportClass(job, TrajectorySegmentWithMetadataWriteSupport.class);
        JavaPairRDD segmentedTrajectoriesRDD = trajectoriesRDD.flatMapToPair(f-> {
            
            String objectId = f.getObjectId();
            SpatioTemporalPoint[] spts = f.getSpatioTemporalPoints();
            
            List<Tuple2<Long, TrajectorySegmentWithMetadata>> trajectoryParts = new ArrayList<>();
            List<SpatioTemporalPoint> currentPart = new ArrayList<>();

            //initialize for the currentHilValue
            int part = 1;
            long intervalStart = 1;
            long intervalEnd = 2;
            long[] hil1 = indexUtils.scale(spts[0]);//indexType.equals("3D")?HilbertUtil.scaleGeoTemporalPoint(spts[0].getLongitude(), minLon, maxLon, spts[0].getLatitude(), minLat, maxLat, spts[0].getTimestamp(), minTime, maxTime, maxOrdinates):HilbertUtil.scaleGeoPoint(spts[0].getLongitude(), minLon, maxLon, spts[0].getLatitude(), minLat, maxLat, maxOrdinates);
            Ranges ranges = ((SmallHilbertCurve)smallHilbertCurveBr.getValue()).query(hil1, hil1, 0);
            long currentHilValue = ranges.toList().get(0).low();
            currentPart.add(new SpatioTemporalPoint(spts[0].getLongitude(), spts[0].getLatitude(),spts[0].getTimestamp()));

            for (int i = 1; i < spts.length; i++) {
                long[] hil2 = indexUtils.scale(spts[i]);//indexType.equals("3D")?HilbertUtil.scaleGeoTemporalPoint(spts[i].getLongitude(), minLon, maxLon, spts[i].getLatitude(), minLat, maxLat, spts[i].getTimestamp(), minTime, maxTime, maxOrdinates):HilbertUtil.scaleGeoPoint(spts[i].getLongitude(), minLon, maxLon, spts[i].getLatitude(), minLat, maxLat, maxOrdinates);
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

                                Optional<STPoint[]> stPoints = indexUtils.clipping(cube,spts[i-1],spts[i]);
//                                double xMin = minLon + (cube[0] * (maxLon-minLon)/(maxOrdinates+ 1L));
//                                double yMin = minLat + (cube[1] * (maxLat-minLat)/(maxOrdinates+ 1L));
//
//                                double xMax = minLon + ((cube[0]+1) * (maxLon-minLon)/(maxOrdinates+ 1L));
//                                double yMax = minLat + ((cube[1]+1) * (maxLat-minLat)/(maxOrdinates+ 1L));
//
//                                Optional<STPoint[]> stPoints;
//                                if(indexType.equals("3D")){
//                                    long tMin = minTime + (cube[2] * (maxTime-minTime)/(maxOrdinates+ 1L));
//                                    long tMax = minTime + ((cube[2]+1) * (maxTime-minTime)/(maxOrdinates+ 1L));
//                                    stPoints = HilbertUtil.liangBarsky(spts[i-1].getLongitude(), spts[i-1].getLatitude(), spts[i-1].getTimestamp(), spts[i].getLongitude(), spts[i].getLatitude(), spts[i].getTimestamp(), xMin, yMin, tMin, xMax, yMax, tMax);
//                                }else{
//                                    stPoints=HilbertUtil.liangBarskyTimeInterpolation(spts[i-1].getLongitude(), spts[i-1].getLatitude(), spts[i-1].getTimestamp(), spts[i].getLongitude(), spts[i].getLatitude(), spts[i].getTimestamp(), xMin, yMin, xMax, yMax);
//                                }

                                List<Tuple3<Double, Double, Long>> newPoints = new ArrayList<>();
                                if(stPoints.isPresent()){

                                    if(Double.compare(stPoints.get()[0].getX(), spts[i-1].getLongitude())!=0 || Double.compare(stPoints.get()[0].getY(), spts[i-1].getLatitude())!=0 || stPoints.get()[0].getT() != spts[i-1].getTimestamp()){
                                        newPoints.add(new Tuple3<>(stPoints.get()[0].getX(), stPoints.get()[0].getY(), stPoints.get()[0].getT()));
                                    }

                                    if(Double.compare(stPoints.get()[1].getX(),spts[i].getLongitude())!=0 || Double.compare(stPoints.get()[1].getY(), spts[i].getLatitude())!=0 || stPoints.get()[1].getT() != spts[i].getTimestamp()){
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

                    int z=0;
                    if(part!=1){z=1;}
                    for (int j=z; j < currentPart.size()-1; j++) {
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

                    List<SpatialPoint> pivots = new ArrayList<>(3);
                    if(currentPart.size()>2) {
                        SpatialPoint medoid = findMedoid(currentPart, (minLongitude + maxLongitude) / 2, (minLatitude + maxLatitude) / 2);
                        SpatialPoint fartherFromMedoid = findFartherFrom(currentPart, medoid);
                        SpatialPoint farther = findFartherFrom(currentPart, fartherFromMedoid);

                        pivots.add(medoid);
                        if (!pivots.contains(fartherFromMedoid)) {
                            pivots.add(fartherFromMedoid);
                        }
                        if (!pivots.contains(farther)) {
                            pivots.add(farther);
                        }
                    }
                    if(part==1){
                        SpatialPoint firstPoint = new SpatialPoint(currentPart.get(0).getLongitude(), currentPart.get(0).getLatitude());
                        if(!pivots.contains(firstPoint)){
                            pivots.add(firstPoint);
                        }else{
                            pivots.remove(firstPoint);
                            pivots.add(firstPoint);
                        }
                    }

                    trajectoryParts.add(Tuple2.apply(currentHilValue, TrajectorySegmentWithMetadata.newTrajectorySegmentWithMetadata( new TrajectorySegment(objectId, part++, currentPart.toArray(new SpatioTemporalPoint[0]), minLongitude, minLatitude, minTimestamp, maxLongitude, maxLatitude, maxTimestamp), pivots.toArray(new SpatialPoint[0]), new long[]{intervalStart, intervalEnd-1} )));
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
                        trajectoryParts.add(Tuple2.apply(passingSegment._1, TrajectorySegmentWithMetadata.newTrajectorySegmentWithMetadata(new TrajectorySegment(objectId, part++, passingSegment._2, Math.min(passingSegment._2[0].getLongitude(), passingSegment._2[1].getLongitude()), Math.min(passingSegment._2[0].getLatitude(), passingSegment._2[1].getLatitude()), passingSegment._2[0].getTimestamp(), Math.max(passingSegment._2[0].getLongitude(), passingSegment._2[1].getLongitude()), Math.max(passingSegment._2[0].getLatitude(), passingSegment._2[1].getLatitude()), passingSegment._2[1].getTimestamp()), null, null)));
                    }

                    for (int i1 = 1; i1 < passingSegments.size(); i1++) {
                        if(passingSegments.get(i1-1)._2[1].getTimestamp() != passingSegments.get(i1)._2[0].getTimestamp() || passingSegments.get(i1-1)._2[1].getLongitude() != passingSegments.get(i1)._2[0].getLongitude() || passingSegments.get(i1-1)._2[1].getLatitude() != passingSegments.get(i1)._2[0].getLatitude()){
                            passingSegments.forEach(pair-> System.out.println(pair._2[0] + "-"+pair._2[1]));
                            throw new Exception("Problem with the passing segments list. A point seems not to be the same with the first point in the next segment. Object id: "+objectId+" Points: "+passingSegments.get(i1-1)._2[1] + " "+passingSegments.get(i1)._2[0]);
                        }
                    }

                    passingSegments.clear();

                    if(pointBegin!=null) {
                        currentPart.add(pointBegin);
                    }else{
                        throw new Exception("Point last should exist, but it is "+pointBegin);
                    }

                    currentPart.add(new SpatioTemporalPoint(spts[i].getLongitude(), spts[i].getLatitude(), spts[i].getTimestamp()));

                    currentHilValue = hilbertValue;
                    hil1 = hil2;
                    intervalStart = intervalEnd;
                }else{
                    currentPart.add(new SpatioTemporalPoint(spts[i].getLongitude(), spts[i].getLatitude(),spts[i].getTimestamp()));
                }
                intervalEnd++;
            }

            //leftovers in the currentPartList
            if(currentPart.size()>0){
                double minLongitude = Double.MAX_VALUE;
                double minLatitude = Double.MAX_VALUE;
                long minTimestamp = Long.MAX_VALUE;

                double maxLongitude = -Double.MAX_VALUE;
                double maxLatitude = -Double.MAX_VALUE;
                long maxTimestamp = Long.MIN_VALUE;

                int z=0;
                if(part!=1){z=1;}
                for (int j=z; j < currentPart.size(); j++) {
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

                List<SpatialPoint> pivots = new ArrayList<>(3);
                if(currentPart.size()>2) {
                    SpatialPoint medoid = findMedoid(currentPart, (minLongitude + maxLongitude) / 2, (minLatitude + maxLatitude) / 2);
                    SpatialPoint fartherFromMedoid = findFartherFrom(currentPart, medoid);
                    SpatialPoint farther = findFartherFrom(currentPart, fartherFromMedoid);

                    pivots.add(medoid);
                    if (!pivots.contains(fartherFromMedoid)) {
                        pivots.add(fartherFromMedoid);
                    }
                    if (!pivots.contains(farther)) {
                        pivots.add(farther);
                    }
                }
                if(part==1){
                    SpatialPoint firstPoint = new SpatialPoint(currentPart.get(0).getLongitude(), currentPart.get(0).getLatitude());
                    if(!pivots.contains(firstPoint)){
                        pivots.add(firstPoint);
                    }else{
                        pivots.remove(firstPoint);
                        pivots.add(firstPoint);
                    }
                }

                trajectoryParts.add(Tuple2.apply(currentHilValue, TrajectorySegmentWithMetadata.newTrajectorySegmentWithMetadata(new TrajectorySegment(objectId, part++, currentPart.toArray(new SpatioTemporalPoint[0]), minLongitude, minLatitude, minTimestamp, maxLongitude, maxLatitude, maxTimestamp), pivots.toArray(new SpatialPoint[0]), new long[]{intervalStart, intervalEnd-1})));

                currentPart.clear();
            }

            trajectoryParts.sort(Comparator.comparingLong(d-> d._2.getTrajectorySegment().getSpatioTemporalPoints()[0].getTimestamp()));
            for (int i = 0; i < trajectoryParts.size()-1; i++) {
                if(trajectoryParts.get(i)._2.getTrajectorySegment().getSpatioTemporalPoints()[trajectoryParts.get(i)._2.getTrajectorySegment().getSpatioTemporalPoints().length-1].getTimestamp() != trajectoryParts.get(i+1)._2.getTrajectorySegment().getSpatioTemporalPoints()[0].getTimestamp() ||
                        trajectoryParts.get(i)._2.getTrajectorySegment().getSpatioTemporalPoints()[trajectoryParts.get(i)._2.getTrajectorySegment().getSpatioTemporalPoints().length-1].getLongitude() != trajectoryParts.get(i+1)._2.getTrajectorySegment().getSpatioTemporalPoints()[0].getLongitude() ||
                        trajectoryParts.get(i)._2.getTrajectorySegment().getSpatioTemporalPoints()[trajectoryParts.get(i)._2.getTrajectorySegment().getSpatioTemporalPoints().length-1].getLatitude() != trajectoryParts.get(i+1)._2.getTrajectorySegment().getSpatioTemporalPoints()[0].getLatitude())
                {
                    System.out.println(trajectoryParts.get(i)._2.getTrajectorySegment().getSegment()+" - "+trajectoryParts.get(i)._2.getTrajectorySegment().getSpatioTemporalPoints().length+" "+trajectoryParts.get(i+1)._2.getTrajectorySegment().getSpatioTemporalPoints().length+" "+trajectoryParts.get(i)._2.getTrajectorySegment().getSpatioTemporalPoints()[trajectoryParts.get(i)._2.getTrajectorySegment().getSpatioTemporalPoints().length-1].getTimestamp() +" "+ trajectoryParts.get(i+1)._2.getTrajectorySegment().getSpatioTemporalPoints()[0].getTimestamp() +" "+
                            trajectoryParts.get(i)._2.getTrajectorySegment().getSpatioTemporalPoints()[trajectoryParts.get(i)._2.getTrajectorySegment().getSpatioTemporalPoints().length-1].getLongitude() +" "+ trajectoryParts.get(i+1)._2.getTrajectorySegment().getSpatioTemporalPoints()[0].getLongitude() +" "+
                            trajectoryParts.get(i)._2.getTrajectorySegment().getSpatioTemporalPoints()[trajectoryParts.get(i)._2.getTrajectorySegment().getSpatioTemporalPoints().length-1].getLatitude() +" "+ trajectoryParts.get(i+1)._2.getTrajectorySegment().getSpatioTemporalPoints()[0].getLatitude());
                    throw new Exception("Problem concerning the linking of the points in the trajectory segments. Object id: "+objectId+" Total trajectory segments "+trajectoryParts.size());
                }
            }

            for (int i = 0; i < trajectoryParts.size()-1; i++) {
                if(trajectoryParts.get(i)._2.getTrajectorySegment().getSegment() + 1 != trajectoryParts.get(i+1)._2.getTrajectorySegment().getSegment()){
                    throw new Exception("Problem concerning the numbering of trajectory segments.");
                }
            }

            Tuple2<Long, TrajectorySegmentWithMetadata> trjSeg = trajectoryParts.get(trajectoryParts.size()-1);
            List<SpatialPoint> pivots = new ArrayList<>(Arrays.asList(trjSeg._2().getPivots()));
            SpatioTemporalPoint lp = trjSeg._2.getTrajectorySegment().getSpatioTemporalPoints()[trjSeg._2.getTrajectorySegment().getSpatioTemporalPoints().length-1];
            SpatialPoint lastPoint = new SpatialPoint(lp.getLongitude(), lp.getLatitude());
            if(!pivots.contains(lastPoint)){
                pivots.add(lastPoint);
            }else{
                if(!pivots.get(pivots.size()-1).equals(lastPoint)){
                    pivots.remove(lastPoint);
                }
                pivots.add(lastPoint);
            }
            Tuple2<Long, TrajectorySegmentWithMetadata> newTrjSeg = new Tuple2<>(trjSeg._1, TrajectorySegmentWithMetadata.newTrajectorySegmentWithMetadata(new TrajectorySegment(trjSeg._2.getTrajectorySegment().getObjectId(), -1* trjSeg._2.getTrajectorySegment().getSegment(), trjSeg._2.getTrajectorySegment().getSpatioTemporalPoints(), trjSeg._2.getTrajectorySegment().getMinLongitude(), trjSeg._2.getTrajectorySegment().getMinLatitude(), trjSeg._2.getTrajectorySegment().getMinTimestamp(), trjSeg._2.getTrajectorySegment().getMaxLongitude(), trjSeg._2.getTrajectorySegment().getMaxLatitude(), trjSeg._2.getTrajectorySegment().getMaxTimestamp()), pivots.toArray(new SpatialPoint[0]), new long[]{trjSeg._2.getInterval()[0], trjSeg._2.getInterval()[1]*(-1)}));
            trajectoryParts.set(trajectoryParts.size()-1, newTrjSeg);


//            if(trajectoryParts.size()==7){
//            TrajectorySegment t = trajectoryParts.get(6)._2.getTrajectorySegment();
//            if(t.getSegment()==-7 && t.getSpatioTemporalPoints().length==4){
//                System.out.println(Arrays.toString(t.getSpatioTemporalPoints()));
//                System.out.println("mbr: "+ t.getMinLongitude()+" "+ t.getMinLatitude()+" "+t.getMinTimestamp()+ " - "+t.getMaxLongitude()+" "+ t.getMaxLatitude()+" "+t.getMaxTimestamp());
//                System.exit(1);
//            }}

//            if(trajectoryParts.get(trajectoryParts.size()-1)._2.getTrajectorySegment().getSegment()<-1 && trajectoryParts.get(trajectoryParts.size()-1)._2.getPivots().length>3){
//                System.out.println(Arrays.toString(trajectoryParts.get(trajectoryParts.size()-1)._2.getTrajectorySegment().getSpatioTemporalPoints()));
//                System.out.println("pivots: "+ Arrays.toString(trajectoryParts.get(trajectoryParts.size()-1)._2.getPivots()));
//                System.exit(1);
//            }


//            if(intervalEnd-1!=spts.length){
//                System.exit(1);
//            }
//            for (Tuple2<Long, TrajectorySegmentWithMetadata> trajectoryPart : trajectoryParts) {
//                if(trajectoryPart._2.getTrajectorySegment().getSegment()>1){
//                    if(trajectoryPart._2.getTrajectorySegment().getSpatioTemporalPoints().length>2){
//                        if(!(trajectoryPart._2.getInterval()[1]- trajectoryPart._2.getInterval()[0]+1==trajectoryPart._2.getTrajectorySegment().getSpatioTemporalPoints().length-2)){
//                            System.exit(1);
//                        }
//                    }else{
//                        if(trajectoryPart._2.getInterval()!=null){
//                            System.exit(1);
//                        }
//                    }
//                }
//            }
//            long y = trajectoryParts.get(0)._2.getInterval()[1];
//            for (int i = 1; i < trajectoryParts.size(); i++) {
//                if(trajectoryParts.get(i)._2.getTrajectorySegment().getSpatioTemporalPoints().length>2){
//                    if(y+1 != trajectoryParts.get(i)._2.getInterval()[0])
//                    {
//                        System.out.println(y+" "+trajectoryParts.get(i)._2.getInterval()[0]);
//                        System.exit(1);
//                    }
//                    y=trajectoryParts.get(i)._2.getInterval()[1];
//                }
//            }
//
//            for (int i = 1; i < trajectoryParts.size()-1; i++) {
//                if(trajectoryParts.get(i)._2.getTrajectorySegment().getSpatioTemporalPoints().length==2 && trajectoryParts.get(i)._2.getInterval()!=null){
//                    System.exit(1);
//                }
//            }
//
////            if(trajectoryParts.get(0)._2.getTrajectorySegment().getSegment()==-1){
////                System.out.println(trajectoryParts.get(0)._2.getTrajectorySegment());
////                System.out.println(Arrays.toString(trajectoryParts.get(0)._2.getInterval()));
////                System.exit(1);
////            }
//            for (Tuple2<Long, TrajectorySegmentWithMetadata> trajectoryPart : trajectoryParts) {
//                if(trajectoryPart._2.getPivots()==null && trajectoryPart._2.getInterval()!=null){
//                    System.exit(1);
//                }
//                if(trajectoryPart._2.getPivots()!=null && trajectoryPart._2.getInterval()==null){
//                    System.exit(1);
//                }
//            }
            return trajectoryParts.iterator();

        }).mapToPair((t)->{return Tuple2.apply(new HilbertKeyTimestamp(t._1, t._2.getTrajectorySegment().getMinTimestamp()),t._2);}).repartitionAndSortWithinPartitions(new HilbertKeyPartitioner(Integer.parseInt(args[0]))).mapToPair(f->Tuple2.apply(Tuple2.apply(f._1.getHilbertKey()+"/", null), f._2));
        segmentedTrajectoriesRDD.saveAsNewAPIHadoopFile(writePath+File.separator+"stIndex", Void.class, TrajectorySegmentWithMetadata.class, MultipleParquetOutputsFormat.class, job.getConfiguration());

        Tuple2<Long, Long> stats = ((JavaPairRDD<Tuple2<String, Object>,TrajectorySegmentWithMetadata>)segmentedTrajectoriesRDD).mapToPair(f->{return Tuple2.apply(f._2.getTrajectorySegment().getObjectId(), f._1._1);}).groupByKey().mapValues(f->{
            HashSet<String> set = new HashSet<>();
            for (String s : f) {
                set.add(s);
            }
            return set.size();
        }).values().aggregate(
                new Tuple2<>(0L, 0L),
                (acc, v) -> new Tuple2<>(acc._1 + v, acc._2 + 1),
                (a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2)
        );

        Tuple2<Long, Long> stats1 = trajectoriesRDD.map(f->{
            HashSet<Long> set = new HashSet<>();
            for (SpatioTemporalPoint spatioTemporalPoint : f.getSpatioTemporalPoints()) {
                long[] hil = indexUtils.scale(spatioTemporalPoint);
                set.add(((SmallHilbertCurve)smallHilbertCurveBr.getValue()).index(hil));
            }
            return set.size();
        }).aggregate(
                new Tuple2<>(0L, 0L),
                (acc, v) -> new Tuple2<>(acc._1 + v, acc._2 + 1),
                (a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2)
        );


        long endTime = System.currentTimeMillis();
        System.out.println("Exec Time: "+(endTime-startTime));

        Config metadataFile = ConfigFactory.empty()
                .withValue("gridHilbert.indexType", ConfigValueFactory.fromAnyRef(indexType))
                .withValue("gridHilbert.bits", ConfigValueFactory.fromAnyRef(bits))
                .withValue("gridHilbert.boundaries.minLon", ConfigValueFactory.fromAnyRef(minLon))
                .withValue("gridHilbert.boundaries.minLat", ConfigValueFactory.fromAnyRef(minLat))
                .withValue("gridHilbert.boundaries.minTime", ConfigValueFactory.fromAnyRef(minTime))
                .withValue("gridHilbert.boundaries.maxLon", ConfigValueFactory.fromAnyRef(maxLon))
                .withValue("gridHilbert.boundaries.maxLat", ConfigValueFactory.fromAnyRef(maxLat))
                .withValue("gridHilbert.boundaries.maxTime", ConfigValueFactory.fromAnyRef(maxTime))
                .withValue("gridHilbert.averageIntersectedCellsPerTrajectory", ConfigValueFactory.fromAnyRef((double) stats._1 / stats._2))
                .withValue("gridHilbert.averageIntersectedCellsPerPointTrajectory", ConfigValueFactory.fromAnyRef((double) stats1._1 / stats1._2))
                .withValue("gridHilbert.numOfTrajectories", ConfigValueFactory.fromAnyRef(stats._2));

        String json = metadataFile.root().render(
                ConfigRenderOptions.defaults()
                        .setJson(false)
                        .setFormatted(true).setComments(false).setOriginComments(false)

        );

        if(writePath.startsWith("hdfs://")){
            FileSystem fs = FileSystem.get(job.getConfiguration());
            try (FSDataOutputStream out = fs.create(new Path(writePath+"/"+"space.metadata"), true)) {
                out.write(json.getBytes(StandardCharsets.UTF_8));
            }
        }else{
            try (FileWriter fw = new FileWriter(writePath+ File.separator+"space.metadata")) {
                fw.write(json);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try(BufferedWriter bf = new BufferedWriter(new FileWriter(metricsPathExport+File.separator+"data-loading-trajparquetDirectoriesWithWholeTrajectories-"+Paths.get(writePath).getFileName().toString()+".txt"))) {
            bf.write("Write Time");
            bf.newLine();
            bf.write(String.valueOf((endTime - startTime)/1000));
        }

        sparkSession.close();
    }

    private static SpatialPoint findMedoid(List<SpatioTemporalPoint> spatioTemporalPoints, double centroidLon,double centroidLat){
        double minDist = Double.MAX_VALUE;
        SpatioTemporalPoint medoid = null;
        for (int i = 1; i < spatioTemporalPoints.size()-1; i++) {
            double distance = HilbertUtil.euclideanDistance(spatioTemporalPoints.get(i).getLongitude(),spatioTemporalPoints.get(i).getLatitude(),centroidLon,centroidLat);
            if (Double.compare(distance,minDist)==-1) {
                medoid = spatioTemporalPoints.get(i);
                minDist = distance;
            }
        }
//        if(medoid==null){
//            System.out.println("is: "+spatioTemporalPoints.size()+" "+part+" "+spatioTemporalPoints.get(0)+" "+spatioTemporalPoints.get(1)+" ");
//        }
        return new SpatialPoint(medoid.getLongitude(), medoid.getLatitude());
    }

    private static SpatialPoint findFartherFrom(List<SpatioTemporalPoint> spatioTemporalPoints, SpatialPoint spatialPoint){
        double maxDist = -Double.MAX_VALUE;
        SpatioTemporalPoint fartherFrom = null;
        for (int i = 1; i < spatioTemporalPoints.size()-1; i++) {
            double distance = HilbertUtil.euclideanDistance(spatioTemporalPoints.get(i).getLongitude(),spatioTemporalPoints.get(i).getLatitude(),spatialPoint.getLongitude(),spatialPoint.getLatitude());
            if (Double.compare(distance,maxDist)==1) {
                fartherFrom = spatioTemporalPoints.get(i);
                maxDist = distance;
            }
        }
        return new SpatialPoint(fartherFrom.getLongitude(), fartherFrom.getLatitude());
    }
}
