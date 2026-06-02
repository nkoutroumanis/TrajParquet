package gr.ds.unipi.spatialnodb.queries.trajparquet;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.messages.common.IndexUtils;
import gr.ds.unipi.spatialnodb.messages.common.IndexUtils2D;
import gr.ds.unipi.spatialnodb.messages.common.IndexUtils3D;
import gr.ds.unipi.spatialnodb.messages.common.SpatioTemporalPoint;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.*;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.pathReadParquet.ParquetInputFormatWithKey;
import gr.ds.unipi.spatialnodb.shapes.STPoint;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.Range;
import org.davidmoten.hilbert.Ranges;
import org.davidmoten.hilbert.SmallHilbertCurve;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

import static gr.ds.unipi.spatialnodb.AppConfig.loadConfig;

public class BatchQueriesDirectoriesRange {
    public static void main(String args[]) throws IOException {

        Config config = loadConfig("queries.conf");

        Config dataLoading = config.getConfig("queries");
        final String parquetPath = dataLoading.getString("parquetPath");
        final String queriesFilePath = dataLoading.getString("queriesFilePath");
        final String metricsPath = dataLoading.getString("metricsPath");

        Config metadata = ConfigFactory.parseFile(new File(parquetPath+ File.separator+"space.metadata")).resolve().getConfig("gridHilbert");
        final int bits = metadata.getInt("bits");
        Config boundaries = metadata.getConfig("boundaries");
        final double minLon = boundaries.getDouble("minLon");
        final double minLat = boundaries.getDouble("minLat");
        final long minTime = boundaries.getLong("minTime");
        final double maxLon = boundaries.getDouble("maxLon");
        final double maxLat = boundaries.getDouble("maxLat");
        final long maxTime = boundaries.getLong("maxTime");
        final String indexType = metadata.getString("indexType");

        final SmallHilbertCurve hilbertCurve = HilbertCurve.small().bits(bits).dimensions(indexType.equals("3D")?3:2);
        final long maxOrdinates = hilbertCurve.maxOrdinate();

        final IndexUtils indexUtils;
        if(!(indexType.equals("2D") || indexType.equals("3D"))) {
            throw new IllegalArgumentException("The index parameter must be either 2D or 3D");
        }
        if(indexType.equals("3D")) {
            indexUtils = new IndexUtils3D(minLon, minLat, minTime, maxLon, maxLat, maxTime, maxOrdinates);
        }else {
            indexUtils = new IndexUtils2D(minLon, minLat, maxLon, maxLat, maxOrdinates);
        }

        Job job = Job.getInstance();

        ParquetInputFormat.setReadSupportClass(job, TrajectorySegmentReadSupport.class);

        SparkConf sparkConf = new SparkConf().registerKryoClasses(new Class[]{SpatioTemporalPoint.class, SpatioTemporalPoint[].class, HilbertUtil.class});
        sparkConf.setAppName("Batch Range Querying in TrajParquet");
        if (!sparkConf.contains("spark.master")) {
            sparkConf.setMaster("local[*]").set("spark.executor.memory","4g");
        }
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        long startTime = System.currentTimeMillis();

        Broadcast br =  jsc.broadcast(hilbertCurve);

        File[] directories = new File(parquetPath+ File.separator+"stIndex").listFiles(File::isDirectory);
        Set<String> directoriesSet = new HashSet<>();
        for (File directory : directories) {
            directoriesSet.add(directory.getName());
        }

        Function<String, Tuple2<STPoint, STPoint>> readRangeQueriesFunction = new Function<String, Tuple2<STPoint, STPoint>>() {
            @Override
            public Tuple2<STPoint, STPoint> call(String query) {

                String[] queryParts = query.split(";");
                double queryMinLongitude = Double.parseDouble(queryParts[0]);
                double queryMinLatitude = Double.parseDouble(queryParts[1]);
                long queryMinTimestamp = Long.parseLong(queryParts[2]);

                double queryMaxLongitude = Double.parseDouble(queryParts[3]);
                double queryMaxLatitude = Double.parseDouble(queryParts[4]);
                long queryMaxTimestamp = Long.parseLong(queryParts[5]);
                return new Tuple2<>(new STPoint(queryMinLongitude, queryMinLatitude, queryMinTimestamp), new STPoint(queryMaxLongitude, queryMaxLatitude, queryMaxTimestamp));
            }
        };

        PairFlatMapFunction<Tuple2<Long, Tuple2<STPoint, STPoint>>, Long, Tuple2<Long,Tuple2<STPoint, STPoint> >> queryToIntersectedCubesFunction = tuple2 -> {

            SmallHilbertCurve hilbertCurveBr = ((SmallHilbertCurve) br.getValue());

            final double queryMinLongitude = Double.max(minLon, tuple2._2._1.getX());
            final double queryMinLatitude = Double.max(minLat, tuple2._2._1.getY());
            final long queryMinTimestamp = Long.max(minTime, tuple2._2._1.getT());

            final double queryMaxLongitude = Double.min(maxLon, tuple2._2._2.getX());
            final double queryMaxLatitude = Double.min(maxLat, tuple2._2._2.getY());
            final long queryMaxTimestamp = Long.min(maxTime-1000, tuple2._2._2.getT());

            long[] hilStart = indexUtils.scale(queryMinLongitude, queryMinLatitude, queryMinTimestamp);//HilbertUtil.scaleGeoTemporalPoint(queryMinLongitude, minLon, maxLon,queryMinLatitude, minLat, maxLat, queryMinTimestamp, minTime, maxTime, maxOrdinates);
            long[] hilEnd = indexUtils.scale(queryMaxLongitude, queryMaxLatitude, queryMaxTimestamp);//HilbertUtil.scaleGeoTemporalPoint(queryMaxLongitude, minLon, maxLon, queryMaxLatitude, minLat, maxLat, queryMaxTimestamp, minTime, maxTime, maxOrdinates);
            Ranges ranges = hilbertCurveBr.query(hilStart, hilEnd, 0);

            List<Tuple2<Long, Tuple2<Long,Tuple2<STPoint, STPoint> >>> cubes = new ArrayList<>();

//            ranges.toList().forEach(range -> {
//                for (long r = range.low(); r <= range.high(); r++) {
//                    if(directoriesSet.contains(String.valueOf(r))) {
//                        long[] arr = hilbertCurveBr.point(r);
//                        if(arr[0] == hilStart[0] || arr[1] == hilStart[1] || arr[2] == hilStart[2]
//                                || arr[0] == hilEnd[0] || arr[1] == hilEnd[1] || arr[2] == hilEnd[2]) {
//                            cubes.add(Tuple2.apply(r,Tuple2.apply(tuple2._1,tuple2._2)));
//                        }
//                    }
//                }
//            });

            for (Range range : ranges.toList()) {
                for (long r = range.low(); r <= range.high(); r++) {
                    if(directoriesSet.contains(String.valueOf(r))) {
                        long[] arr = hilbertCurveBr.point(r);
                        for (int i = 0; i < arr.length; i++) {
                            if(arr[i] == hilStart[i] || arr[i] == hilEnd[i]) {
                                cubes.add(Tuple2.apply(r,Tuple2.apply(tuple2._1,tuple2._2)));
                                break;
                            }
                        }
                    }
                }
            }

            return cubes.iterator();
        };

        PairFlatMapFunction<Tuple2<Long, Tuple2<STPoint, STPoint>>, Long, Long> queryToFullyContainedCubesFunctionOnlyId = tuple2 -> {

            SmallHilbertCurve hilbertCurveBr = ((SmallHilbertCurve) br.getValue());

            final double queryMinLongitude = Double.max(minLon, tuple2._2._1.getX());
            final double queryMinLatitude = Double.max(minLat, tuple2._2._1.getY());
            final long queryMinTimestamp = Long.max(minTime, tuple2._2._1.getT());

            final double queryMaxLongitude = Double.min(maxLon, tuple2._2._2.getX());
            final double queryMaxLatitude = Double.min(maxLat, tuple2._2._2.getY());
            final long queryMaxTimestamp = Long.min(maxTime-1000, tuple2._2._2.getT());

            long[] hilStart = indexUtils.scale(queryMinLongitude, queryMinLatitude, queryMinTimestamp);//HilbertUtil.scaleGeoTemporalPoint(queryMinLongitude, minLon, maxLon,queryMinLatitude, minLat, maxLat, queryMinTimestamp, minTime, maxTime, maxOrdinates);
            long[] hilEnd = indexUtils.scale(queryMaxLongitude, queryMaxLatitude, queryMaxTimestamp);//HilbertUtil.scaleGeoTemporalPoint(queryMaxLongitude, minLon, maxLon, queryMaxLatitude, minLat, maxLat, queryMaxTimestamp, minTime, maxTime, maxOrdinates);
            Ranges ranges = hilbertCurveBr.query(hilStart, hilEnd, 0);

            List<Tuple2<Long, Long>> cubes = new ArrayList<>();

            boolean flag = true;
            for (Range range : ranges.toList()) {
                for (long r = range.low(); r <= range.high(); r++) {
                    if(directoriesSet.contains(String.valueOf(r))) {
                        long[] arr = hilbertCurveBr.point(r);
                        for (int i = 0; i < arr.length; i++) {
                            if(arr[i] == hilStart[i] || arr[i] == hilEnd[i]) {
                                flag = false;
                                break;
                            }
                        }
                        if(flag) {
                            cubes.add(Tuple2.apply(r, tuple2._1));
                        }
                        flag = true;
                    }
                }
            }
            return cubes.iterator();
        };

        PairFlatMapFunction<Tuple2<Long, Tuple2<STPoint, STPoint>>, Long, Tuple2<Long, Tuple2<STPoint, STPoint>>> queryToFullyContainedCubesFunction = tuple2 -> {

            SmallHilbertCurve hilbertCurveBr = ((SmallHilbertCurve) br.getValue());

            final double queryMinLongitude = Double.max(minLon, tuple2._2._1.getX());
            final double queryMinLatitude = Double.max(minLat, tuple2._2._1.getY());
            final long queryMinTimestamp = Long.max(minTime, tuple2._2._1.getT());

            final double queryMaxLongitude = Double.min(maxLon, tuple2._2._2.getX());
            final double queryMaxLatitude = Double.min(maxLat, tuple2._2._2.getY());
            final long queryMaxTimestamp = Long.min(maxTime-1000, tuple2._2._2.getT());

            long[] hilStart = indexUtils.scale(queryMinLongitude, queryMinLatitude, queryMinTimestamp);//HilbertUtil.scaleGeoTemporalPoint(queryMinLongitude, minLon, maxLon,queryMinLatitude, minLat, maxLat, queryMinTimestamp, minTime, maxTime, maxOrdinates);
            long[] hilEnd = indexUtils.scale(queryMaxLongitude, queryMaxLatitude, queryMaxTimestamp);//HilbertUtil.scaleGeoTemporalPoint(queryMaxLongitude, minLon, maxLon, queryMaxLatitude, minLat, maxLat, queryMaxTimestamp, minTime, maxTime, maxOrdinates);
            Ranges ranges = hilbertCurveBr.query(hilStart, hilEnd, 0);

            List<Tuple2<Long, Tuple2<Long, Tuple2<STPoint, STPoint>>>> cubes = new ArrayList<>();

            boolean flag = true;
            for (Range range : ranges.toList()) {
                for (long r = range.low(); r <= range.high(); r++) {
                    if(directoriesSet.contains(String.valueOf(r))) {
                        long[] arr = hilbertCurveBr.point(r);
                        for (int i = 0; i < arr.length; i++) {
                            if(arr[i] == hilStart[i] || arr[i] == hilEnd[i]) {
                                flag = false;
                                break;
                            }
                        }
                        if(flag) {
                            cubes.add(Tuple2.apply(r, Tuple2.apply(tuple2._1, tuple2._2)));
                        }
                        flag = true;
                    }
                }
            }
            return cubes.iterator();
        };

        PairFlatMapFunction< Tuple2<Long, Tuple2<Tuple2<Long, Tuple2<STPoint, STPoint>> , TrajectorySegment>>, Tuple2<String, Long>, TrajectorySegment> refinementFunction = tuple2 -> {

            List<Tuple2<Tuple2<String, Long>, TrajectorySegment>> trajectoryList = new ArrayList<>();
            List<SpatioTemporalPoint> currentSpatioTemporalPoints = new ArrayList<>();
            long segment = tuple2._2._2.getSegment();

            SpatioTemporalPoint[] spatioTemporalPoints = tuple2._2._2.getSpatioTemporalPoints();
            for (int i = 0; i < spatioTemporalPoints.length - 1; i++) {

                Optional<STPoint[]> stPoints = HilbertUtil.liangBarsky(spatioTemporalPoints[i].getLongitude(), spatioTemporalPoints[i].getLatitude(),spatioTemporalPoints[i].getTimestamp(),
                        spatioTemporalPoints[i+1].getLongitude(), spatioTemporalPoints[i+1].getLatitude(),spatioTemporalPoints[i+1].getTimestamp()
                        ,tuple2._2._1._2._1.getX(), tuple2._2._1._2._1.getY(), tuple2._2._1._2._1.getT(), tuple2._2._1._2._2.getX(), tuple2._2._1._2._2.getY(), tuple2._2._1._2._2.getT());

                if(stPoints.isPresent()){
                    if(stPoints.get().length==2){
                        if(stPoints.get()[0].getT() == spatioTemporalPoints[i].getTimestamp() &&
                                stPoints.get()[1].getT() == spatioTemporalPoints[i+1].getTimestamp()){

                            if(!currentSpatioTemporalPoints.isEmpty()){
                                if(!currentSpatioTemporalPoints.get(currentSpatioTemporalPoints.size()-1).equals(spatioTemporalPoints[i])){
                                    throw new Exception("The i th element of the segment should be the last point of the current list.");
                                }
                            }

                            if (currentSpatioTemporalPoints.isEmpty()) {
                                currentSpatioTemporalPoints.add(new SpatioTemporalPoint(spatioTemporalPoints[i].getLongitude(), spatioTemporalPoints[i].getLatitude(), spatioTemporalPoints[i].getTimestamp()));
                            }
                            currentSpatioTemporalPoints.add(new SpatioTemporalPoint(spatioTemporalPoints[i + 1].getLongitude(), spatioTemporalPoints[i + 1].getLatitude(), spatioTemporalPoints[i + 1].getTimestamp()));

                        }else if(stPoints.get()[0].getT() == spatioTemporalPoints[i].getTimestamp()){

                            if (currentSpatioTemporalPoints.isEmpty()) {
                                currentSpatioTemporalPoints.add(new SpatioTemporalPoint(spatioTemporalPoints[i].getLongitude(), spatioTemporalPoints[i].getLatitude(), spatioTemporalPoints[i].getTimestamp()));
                            }
                            currentSpatioTemporalPoints.add(new SpatioTemporalPoint(stPoints.get()[1].getX(), stPoints.get()[1].getY(), stPoints.get()[1].getT()));
                            trajectoryList.add(Tuple2.apply(Tuple2.apply(tuple2._2._2.getObjectId(), tuple2._2._1._1),new TrajectorySegment(tuple2._2._2.getObjectId(), segment, currentSpatioTemporalPoints.toArray(new SpatioTemporalPoint[0]), 0, 0, 0, 0, 0, 0)));
                            currentSpatioTemporalPoints.clear();
                        }else if(stPoints.get()[1].getT() == spatioTemporalPoints[i+1].getTimestamp()){

                            if(currentSpatioTemporalPoints.size()==1){
                                throw new Exception("Exception for the current list, it will be flushed and has only one element.");
                            }

                            if (!currentSpatioTemporalPoints.isEmpty()) {
                                trajectoryList.add(Tuple2.apply(Tuple2.apply(tuple2._2._2.getObjectId(), tuple2._2._1._1),new TrajectorySegment(tuple2._2._2.getObjectId(), segment, currentSpatioTemporalPoints.toArray(new SpatioTemporalPoint[0]), 0, 0, 0, 0, 0, 0)));
                                currentSpatioTemporalPoints.clear();
                            }
                            currentSpatioTemporalPoints.add(new SpatioTemporalPoint(stPoints.get()[0].getX(), stPoints.get()[0].getY(), stPoints.get()[0].getT()));
                            currentSpatioTemporalPoints.add(new SpatioTemporalPoint(spatioTemporalPoints[i+1].getLongitude(), spatioTemporalPoints[i+1].getLatitude(), spatioTemporalPoints[i+1].getTimestamp()));
                        }else{
                            if(!currentSpatioTemporalPoints.isEmpty()){
                                throw new Exception("The current list has elements while it should not have.");
                            }
                            currentSpatioTemporalPoints.add(new SpatioTemporalPoint(stPoints.get()[0].getX(), stPoints.get()[0].getY(), stPoints.get()[0].getT()));
                            currentSpatioTemporalPoints.add(new SpatioTemporalPoint(stPoints.get()[1].getX(), stPoints.get()[1].getY(), stPoints.get()[1].getT()));
                            trajectoryList.add(Tuple2.apply(Tuple2.apply(tuple2._2._2.getObjectId(), tuple2._2._1._1),new TrajectorySegment(tuple2._2._2.getObjectId(), segment, currentSpatioTemporalPoints.toArray(new SpatioTemporalPoint[0]), 0, 0, 0, 0, 0, 0)));
                            currentSpatioTemporalPoints.clear();
                        }
                    }else{
                        throw new Exception("The array from the Liang Barsky should contain at least one element");
                    }

                }else{
                    if (!currentSpatioTemporalPoints.isEmpty()) {
                        trajectoryList.add(Tuple2.apply(Tuple2.apply(tuple2._2._2.getObjectId(), tuple2._2._1._1) ,new TrajectorySegment(tuple2._2._2.getObjectId(), segment, currentSpatioTemporalPoints.toArray(new SpatioTemporalPoint[0]), 0, 0, 0, 0, 0, 0)));
                        currentSpatioTemporalPoints.clear();
                    }
                }

            }
            if (!currentSpatioTemporalPoints.isEmpty()) {
                trajectoryList.add(Tuple2.apply(Tuple2.apply(tuple2._2._2.getObjectId(), tuple2._2._1._1) ,new TrajectorySegment(tuple2._2._2.getObjectId(), segment, currentSpatioTemporalPoints.toArray(new SpatioTemporalPoint[0]), 0, 0, 0, 0, 0, 0)));
                currentSpatioTemporalPoints.clear();
            }

            return trajectoryList.iterator();
        };

        PairFlatMapFunction<Tuple2<Tuple2<String, Long>, Iterable<TrajectorySegment>>, Long, TrajectorySegment> concatenateTrajectoriesFunction = tuple2 -> {

            List<TrajectorySegment> trSegments = new ArrayList<>();
            tuple2._2.forEach(trSegments::add);

            Comparator<TrajectorySegment> comparator = Comparator.comparingLong(d-> d.getSpatioTemporalPoints()[0].getTimestamp());
            comparator = comparator.thenComparingLong(d-> Math.abs(d.getSegment()));
            trSegments.sort(comparator);

            List<Tuple2<Long, TrajectorySegment>> finalList = new ArrayList<>();
            List<TrajectorySegment> currentMerged = new ArrayList<>();
            currentMerged.add(trSegments.get(0));

            int segmentNum = 0;

            for (int i = 0; i < trSegments.size()-1; i++) {
                SpatioTemporalPoint spatioTemporalPoint1 = trSegments.get(i).getSpatioTemporalPoints()[trSegments.get(i).getSpatioTemporalPoints().length-1];
                SpatioTemporalPoint spatioTemporalPoint2 = trSegments.get(i+1).getSpatioTemporalPoints()[0];

                if(spatioTemporalPoint1.equals(spatioTemporalPoint2)){
                    currentMerged.add(trSegments.get(i+1));
                }else{
                    //clean currentMerged and add to the final list
                    finalList.add(Tuple2.apply(tuple2._1._2,new TrajectorySegment(tuple2._1._1, ++segmentNum, currentMerged)));
                    currentMerged.clear();
                    currentMerged.add(trSegments.get(i+1));
                }
            }

            //leftovers
            if(!currentMerged.isEmpty()){
                finalList.add(Tuple2.apply(tuple2._1._2,new TrajectorySegment(tuple2._1._1,++segmentNum, currentMerged)));
            }
            return finalList.iterator();
        };


        StringBuilder sb = new StringBuilder();
        JavaPairRDD<Long,Tuple2<STPoint, STPoint>> rangeQueries = jsc.textFile(queriesFilePath).map(readRangeQueriesFunction).zipWithUniqueId().mapToPair(i-> Tuple2.apply(i._2, i._1)).cache();
        JavaPairRDD<Tuple2<String, Long>, TrajectorySegment> ungroupedResults = null;

        JavaPairRDD<Long, Tuple2<Long, Tuple2<STPoint, STPoint>>> queriesIntersectedCubes = rangeQueries.<Long, Tuple2<Long, Tuple2<STPoint, STPoint>>>flatMapToPair(queryToIntersectedCubesFunction);
        List<Long> intersectedCubes = queriesIntersectedCubes.keys().distinct().collect();
        if(!intersectedCubes.isEmpty()){
            intersectedCubes.forEach(cube -> sb.append(parquetPath+File.separator+"stIndex").append(File.separator).append(cube).append(","));
            sb.deleteCharAt(sb.length()-1);
            JavaPairRDD<Long, TrajectorySegment> trajectoriesWithIntersectedCubes = (JavaPairRDD<Long, TrajectorySegment>) jsc.newAPIHadoopFile(sb.toString(), ParquetInputFormatWithKey.class, Long.class, TrajectorySegment.class, job.getConfiguration());
            ungroupedResults = queriesIntersectedCubes.join(trajectoriesWithIntersectedCubes).<Tuple2<String, Long>, TrajectorySegment>flatMapToPair(refinementFunction);
        }

        sb.setLength(0);
        JavaPairRDD queriesFullyContainedCubes;
        if(indexUtils instanceof IndexUtils2D){
            //returns JavaPairRDD<Long, Tuple2<ST,ST>> -> first type represents the cell id, the second represents the query id and the two ST points.
            queriesFullyContainedCubes = rangeQueries.<Long, Tuple2<Long, Tuple2<STPoint, STPoint>>>flatMapToPair(queryToFullyContainedCubesFunction);
        }else{
            //returns JavaPairRDD<Long,Long> -> first type represents the cell id, second the query id
            queriesFullyContainedCubes = rangeQueries.<Long, Long>flatMapToPair(queryToFullyContainedCubesFunctionOnlyId);
        }

        List<Long> fullyContainedCubes = queriesFullyContainedCubes.keys().distinct().collect();

        if(!fullyContainedCubes.isEmpty()){
            fullyContainedCubes.forEach(cube -> sb.append(parquetPath+File.separator+"stIndex").append(File.separator).append(cube).append(","));
            sb.deleteCharAt(sb.length()-1);
            JavaPairRDD<Long, TrajectorySegment> trajectoriesWithFullyIntersectedCubes = (JavaPairRDD<Long, TrajectorySegment>) jsc.newAPIHadoopFile(sb.toString(), ParquetInputFormatWithKey.class, Long.class, TrajectorySegment.class, job.getConfiguration());

            JavaPairRDD<Tuple2<String, Long>, TrajectorySegment> resultsFullyContained;
            if(indexUtils instanceof IndexUtils2D){
                resultsFullyContained = ((JavaPairRDD<Long, Tuple2<Long, Tuple2<STPoint, STPoint>>>)queriesFullyContainedCubes).join(trajectoriesWithFullyIntersectedCubes)
                        .<Tuple2<String, Long>, TrajectorySegment>flatMapToPair(f->{

                            List<Tuple2<Tuple2<String,Long>,TrajectorySegment>> trajectoryList = new ArrayList<>();
                            List<SpatioTemporalPoint> currentSpatioTemporalPoints = new ArrayList<>();

                            double minX = f._2._1._2._1.getX();
                            double maxX = f._2._1._2._2.getX();

                            double minY = f._2._1._2._1.getY();
                            double maxY = f._2._1._2._2.getY();

                            long minT = f._2._1._2._1.getT();
                            long maxT = f._2._1._2._2.getT();


                            SpatioTemporalPoint[] spatioTemporalPoints = f._2._2.getSpatioTemporalPoints();
                            boolean previous = false;

                            if(HilbertUtil.inTimeWindow(spatioTemporalPoints[0].getTimestamp(), minT, maxT)){
                                currentSpatioTemporalPoints.add(spatioTemporalPoints[0]);
                                previous = true;
                            }

                            for (int i = 1; i < spatioTemporalPoints.length; i++) {
                                if(previous || HilbertUtil.timeIntervalsIntersect(spatioTemporalPoints[i-1].getTimestamp(),spatioTemporalPoints[i].getTimestamp(),minT,maxT)){
                                    if(previous && HilbertUtil.inTimeWindow(spatioTemporalPoints[i].getTimestamp(), minT, maxT)){
                                        currentSpatioTemporalPoints.add(spatioTemporalPoints[i]);
                                    }else{
                                        Optional<STPoint[]> stPoint = HilbertUtil.liangBarsky(spatioTemporalPoints[i-1].getLongitude(),spatioTemporalPoints[i-1].getLatitude(),spatioTemporalPoints[i-1].getTimestamp(), spatioTemporalPoints[i].getLongitude(),spatioTemporalPoints[i].getLatitude(),spatioTemporalPoints[i].getTimestamp(),minX, minY, minT, maxX, maxY, maxT);
                                        if(previous){
                                            currentSpatioTemporalPoints.add(new SpatioTemporalPoint(stPoint.get()[1].getX(), stPoint.get()[1].getY(), stPoint.get()[1].getT()));
                                            break;
                                        }else if (HilbertUtil.inTimeWindow(spatioTemporalPoints[i].getTimestamp(), minT, maxT)) {
                                            currentSpatioTemporalPoints.add(new SpatioTemporalPoint(stPoint.get()[0].getX(), stPoint.get()[0].getY(), stPoint.get()[0].getT()));
                                            currentSpatioTemporalPoints.add(spatioTemporalPoints[i]);
                                            previous = true;
                                        }else{
                                            currentSpatioTemporalPoints.add(new SpatioTemporalPoint(stPoint.get()[0].getX(), stPoint.get()[0].getY(), stPoint.get()[0].getT()));
                                            currentSpatioTemporalPoints.add(new SpatioTemporalPoint(stPoint.get()[1].getX(), stPoint.get()[1].getY(), stPoint.get()[1].getT()));
                                            break;
                                        }
                                    }
                                }
                            }

                            if (!currentSpatioTemporalPoints.isEmpty()) {
                                trajectoryList.add(Tuple2.apply(Tuple2.apply(f._2._2.getObjectId(), f._2._1._1), new TrajectorySegment(f._2._2.getObjectId(), f._2._2.getSegment(),currentSpatioTemporalPoints.toArray(new SpatioTemporalPoint[0]),0,0,0,0,0,0)));
                                currentSpatioTemporalPoints.clear();
                            }

                            return trajectoryList.iterator();
                });
            }else{
                resultsFullyContained = ((JavaPairRDD<Long,Long>)queriesFullyContainedCubes).join(trajectoriesWithFullyIntersectedCubes).<Tuple2<String, Long>, TrajectorySegment>mapToPair(t->{return Tuple2.apply(Tuple2.apply(t._2._2.getObjectId(), t._2._1), t._2._2);});
            }

            if(ungroupedResults!=null){
                ungroupedResults = ungroupedResults.union(resultsFullyContained);
            }else{
                ungroupedResults = resultsFullyContained;
            }
        }

        if(ungroupedResults!=null){
            JavaPairRDD<Long, TrajectorySegment> results = ungroupedResults.groupByKey().<Long, TrajectorySegment>flatMapToPair(concatenateTrajectoriesFunction);
            System.out.println(results.count());
//            for (Tuple2<Long, Iterable<TrajectorySegment>> longIterableTuple2 : results.groupByKey().sortByKey().collect()) {
//
//                int p = 0;
//                int t=0;
//                for (TrajectorySegment trajectorySegment : longIterableTuple2._2) {
//                    p = p+ trajectorySegment.getSpatioTemporalPoints().length;
//                    t++;
//                }
//                System.out.println(longIterableTuple2._1+";"+t+";"+p);
//            }
        }

        long endTime = System.currentTimeMillis();

        BufferedWriter bw = new BufferedWriter(new FileWriter(metricsPath+ File.separator+"metrics-range-queries-batch-trajparquet-"+ Paths.get(queriesFilePath).getFileName().toString().replaceFirst("\\.[^.]+$", "")+"-"+Paths.get(parquetPath).getFileName().toString()+".txt"));
        bw.write("Total Time (ms)\tTotal Pages\n");
        bw.write((endTime-startTime) + "\t"+ DataPage.counter);
        bw.close();

    }

    private static int countPoints(String line) {
        int count = 0;
        for (int i = 0; i < line.length(); i++) {
            if (line.charAt(i) == ';') count++;
        }
        return count;
    }

    private static double parseDouble(char[] chars, int start, int end) {
        return Double.parseDouble(new String(chars, start, end - start));
    }

    // Parse a long directly from char[] between start (inclusive) and end (exclusive)
    private static long parseLong(char[] chars, int start, int end) {
        long result = 0;
        boolean neg = false;
        int i = start;
        if (chars[i] == '-') { neg = true; i++; }

        while (i < end && chars[i] >= '0' && chars[i] <= '9') {
            result = result * 10 + (chars[i] - '0');
            i++;
        }

        return neg ? -result : result;
    }

}
