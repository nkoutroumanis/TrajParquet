package gr.ds.unipi.spatialnodb.queries.trajparquet;

import com.typesafe.config.Config;
import gr.ds.unipi.spatialnodb.AppConfig;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.SpatioTemporalPoint;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.TrajectorySegment;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.TrajectorySegmentReadSupport;
import gr.ds.unipi.spatialnodb.shapes.STPoint;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.Ranges;
import org.davidmoten.hilbert.SmallHilbertCurve;
import scala.Tuple2;

import java.io.*;
import java.util.*;

import static org.apache.parquet.filter2.predicate.FilterApi.*;

public class RangeQueriesDirectories {
    public static void main(String args[]) throws IOException {

        Config config = AppConfig.newAppConfig(args[0]/*"src/main/resources/app-new.conf"*/).getConfig();

        Config dataLoading = config.getConfig("queries");
        final String parquetPath = dataLoading.getString("parquetPath");
        final String queriesFilePath = dataLoading.getString("queriesFilePath");
        final String queriesFileExport = dataLoading.getString("queriesFileExport");

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

        ParquetInputFormat.setReadSupportClass(job, TrajectorySegmentReadSupport.class);

        SparkConf sparkConf = new SparkConf();//.registerKryoClasses(new Class[]{SpatioTemporalPoint.class,SpatioTemporalPoint[].class});/*.setMaster("local[1]").set("spark.executor.memory","1g")*/
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        File[] directories = new File(parquetPath).listFiles(File::isDirectory);
        Set<String> directoriesSet = new HashSet<>();
        for (File directory : directories) {
            directoriesSet.add(directory.getName());
        }

        List<Long> times = new ArrayList<>();

        BufferedWriter bw = new BufferedWriter(new FileWriter(queriesFileExport));
        BufferedReader br = new BufferedReader(new FileReader(queriesFilePath));
        String query;
        while ((query = br.readLine()) != null) {
            String[] queryParts = query.split(";");
            double queryMinLongitude = Double.parseDouble(queryParts[0]);
            double queryMinLatitude = Double.parseDouble(queryParts[1]);
            long queryMinTimestamp = Long.parseLong(queryParts[2]);

            double queryMaxLongitude = Double.parseDouble(queryParts[3]);
            double queryMaxLatitude = Double.parseDouble(queryParts[4]);
            long queryMaxTimestamp = Long.parseLong(queryParts[5]);

            FilterPredicate xAxis = and(gtEq(doubleColumn("maxLongitude"), queryMinLongitude), ltEq(doubleColumn("minLongitude"), queryMaxLongitude));
            FilterPredicate yAxis = and(gtEq(doubleColumn("maxLatitude"), queryMinLatitude), ltEq(doubleColumn("minLatitude"), queryMaxLatitude));
            FilterPredicate tAxis = and(gtEq(longColumn("maxTimestamp"), queryMinTimestamp), ltEq(longColumn("minTimestamp"), queryMaxTimestamp));

//            ParquetInputFormat.setFilterPredicate(job.getConfiguration(), and(tAxis, and(xAxis, yAxis)));

            long[] hilStart = HilbertUtil.scaleGeoTemporalPoint(queryMinLongitude, minLon, maxLon,queryMinLatitude, minLat, maxLat, queryMinTimestamp, minTime, maxTime, maxOrdinates);
            long[] hilEnd = HilbertUtil.scaleGeoTemporalPoint(queryMaxLongitude, minLon, maxLon, queryMaxLatitude, minLat, maxLat, queryMaxTimestamp, minTime, maxTime, maxOrdinates);
            Ranges ranges = hilbertCurve.query(hilStart, hilEnd, 0);
            StringBuilder sb = new StringBuilder();
            ranges.toList().forEach(range -> {
                for (long r = range.low(); r <= range.high(); r++) {
                    if(directoriesSet.contains(String.valueOf(r))){
                        sb.append(parquetPath+"/"+r+",");
                    }
                }
            });
            if(sb.length()==0){
                bw.write(0+";"+0+";"+0+";"+0);
                DataPage.counter = 0;
                bw.newLine();
                continue;
            }
            sb.deleteCharAt(sb.length()-1);

//            System.out.println("directoriesSet size"+directoriesSet.size());
//            System.out.println("sb.string "+sb.toString());

            long startTime = System.currentTimeMillis();

            JavaPairRDD<Void, TrajectorySegment> pairRDD = (JavaPairRDD<Void, TrajectorySegment>) jsc.newAPIHadoopFile(sb.toString()/*parquetPath*/, ParquetInputFormat.class, Void.class, TrajectorySegment.class, job.getConfiguration());
//            JavaPairRDD<Void, TrajectorySegment> pairRDDRangeQuery = pairRDD;

            JavaPairRDD<Void, TrajectorySegment> pairRDDRangeQuery = (JavaPairRDD<Void, TrajectorySegment>) pairRDD.flatMapValues(f -> {

                List<TrajectorySegment> trajectoryList = new ArrayList<>();
                List<SpatioTemporalPoint> currentSpatioTemporalPoints = new ArrayList<>();
                long segment = 1;

                SpatioTemporalPoint[] spatioTemporalPoints = f.getSpatioTemporalPoints();
                for (int i = 0; i < spatioTemporalPoints.length - 1; i++) {

                    Optional<STPoint[]> stPoints = HilbertUtil.liangBarsky(spatioTemporalPoints[i].getLongitude(), spatioTemporalPoints[i].getLatitude(),spatioTemporalPoints[i].getTimestamp(),
                            spatioTemporalPoints[i+1].getLongitude(), spatioTemporalPoints[i+1].getLatitude(),spatioTemporalPoints[i+1].getTimestamp()
                            ,queryMinLongitude, queryMinLatitude, queryMinTimestamp, queryMaxLongitude, queryMaxLatitude, queryMaxTimestamp);

                    if(stPoints.isPresent()){
//                        if(stPoints.get()[0].getT() == spatioTemporalPoints[i].getTimestamp()&& stPoints.get()[1].getT()== spatioTemporalPoints[i+1].getTimestamp()){
//                            //both points are in query
//                        }else if(stPoints.get()[0].getT() == spatioTemporalPoints[i].getTimestamp()){
//                            //first point of line is in query, second is outside query
//                        }else if(stPoints.get()[1].getT() == spatioTemporalPoints[i+1].getTimestamp()){
//                            //second point of line is in query, first is outside query
//                        }else{
//                            //penetrates the query
//                        }
                        if (currentSpatioTemporalPoints.size() == 0) {
                            currentSpatioTemporalPoints.add(new SpatioTemporalPoint(spatioTemporalPoints[i].getLongitude(), spatioTemporalPoints[i].getLatitude(), spatioTemporalPoints[i].getTimestamp()));
                            currentSpatioTemporalPoints.add(new SpatioTemporalPoint(spatioTemporalPoints[i + 1].getLongitude(), spatioTemporalPoints[i + 1].getLatitude(), spatioTemporalPoints[i + 1].getTimestamp()));
                            } else {
                                currentSpatioTemporalPoints.add(new SpatioTemporalPoint(spatioTemporalPoints[i + 1].getLongitude(), spatioTemporalPoints[i + 1].getLatitude(), spatioTemporalPoints[i + 1].getTimestamp()));
                            }
                    }else{
                        if (currentSpatioTemporalPoints.size() > 0) {
                            trajectoryList.add(new TrajectorySegment(f.getObjectId(), segment++, currentSpatioTemporalPoints.toArray(new SpatioTemporalPoint[0]), 0, 0, 0, 0, 0, 0));
                            currentSpatioTemporalPoints.clear();
                        }
                    }
//                    //if intersected in time
//                    if (spatioTemporalPoints[i + 1].getTimestamp() >= queryMinTimestamp && spatioTemporalPoints[i].getTimestamp() <= queryMaxTimestamp) {
//
//                        //if one of the points of the line is inside the spatial part of the query
//                        if (HilbertUtil.pointInRectangle(spatioTemporalPoints[i].getLongitude(), spatioTemporalPoints[i].getLatitude(), queryMinLongitude, queryMinLatitude, queryMaxLongitude, queryMaxLatitude)
//                                || HilbertUtil.pointInRectangle(spatioTemporalPoints[i + 1].getLongitude(), spatioTemporalPoints[i + 1].getLatitude(), queryMinLongitude, queryMinLatitude, queryMaxLongitude, queryMaxLatitude)) {
//
//                            if (currentSpatioTemporalPoints.size() == 0) {
//                                currentSpatioTemporalPoints.add(new SpatioTemporalPoint(spatioTemporalPoints[i].getLongitude(), spatioTemporalPoints[i].getLatitude(), spatioTemporalPoints[i].getTimestamp()));
//                                currentSpatioTemporalPoints.add(new SpatioTemporalPoint(spatioTemporalPoints[i + 1].getLongitude(), spatioTemporalPoints[i + 1].getLatitude(), spatioTemporalPoints[i + 1].getTimestamp()));
//
//                            } else {
//                                currentSpatioTemporalPoints.add(new SpatioTemporalPoint(spatioTemporalPoints[i + 1].getLongitude(), spatioTemporalPoints[i + 1].getLatitude(), spatioTemporalPoints[i + 1].getTimestamp()));
//                            }
//
//                        } else if (HilbertUtil.lineLineIntersection(spatioTemporalPoints[i].getLongitude(), spatioTemporalPoints[i].getLatitude(), spatioTemporalPoints[i + 1].getLongitude(), spatioTemporalPoints[i + 1].getLatitude(), queryMinLongitude, queryMinLatitude, queryMaxLongitude, queryMinLatitude, true)
//                                || HilbertUtil.lineLineIntersection(spatioTemporalPoints[i].getLongitude(), spatioTemporalPoints[i].getLatitude(), spatioTemporalPoints[i + 1].getLongitude(), spatioTemporalPoints[i + 1].getLatitude(), queryMinLongitude, queryMinLatitude, queryMinLongitude, queryMaxLatitude, true)
//                                || HilbertUtil.lineLineIntersection(spatioTemporalPoints[i].getLongitude(), spatioTemporalPoints[i].getLatitude(), spatioTemporalPoints[i + 1].getLongitude(), spatioTemporalPoints[i + 1].getLatitude(), queryMinLongitude, queryMaxLatitude, queryMaxLongitude, queryMaxLatitude, false)
//                                || HilbertUtil.lineLineIntersection(spatioTemporalPoints[i].getLongitude(), spatioTemporalPoints[i].getLatitude(), spatioTemporalPoints[i + 1].getLongitude(), spatioTemporalPoints[i + 1].getLatitude(), queryMaxLongitude, queryMinLatitude, queryMaxLongitude, queryMaxLatitude, false)) {
//                            //if the line penetrates the spatial part of the query
//                            if (currentSpatioTemporalPoints.size() == 0) {
//                                currentSpatioTemporalPoints.add(new SpatioTemporalPoint(spatioTemporalPoints[i].getLongitude(), spatioTemporalPoints[i].getLatitude(), spatioTemporalPoints[i].getTimestamp()));
//                                currentSpatioTemporalPoints.add(new SpatioTemporalPoint(spatioTemporalPoints[i + 1].getLongitude(), spatioTemporalPoints[i + 1].getLatitude(), spatioTemporalPoints[i + 1].getTimestamp()));
//
//                            } else {
//                                currentSpatioTemporalPoints.add(new SpatioTemporalPoint(spatioTemporalPoints[i + 1].getLongitude(), spatioTemporalPoints[i + 1].getLatitude(), spatioTemporalPoints[i + 1].getTimestamp()));
//                            }
//                        } else {//if the line does not intersect with the spatial part of the query
//                            if (currentSpatioTemporalPoints.size() > 0) {
//                                trajectoryList.add(new TrajectorySegment(f.getObjectId(), segment++, currentSpatioTemporalPoints.toArray(new SpatioTemporalPoint[0]), 0, 0, 0, 0, 0, 0));
//                                currentSpatioTemporalPoints.clear();
//                            }
//                        }
//                    }

                }
                if (currentSpatioTemporalPoints.size() > 0) {
                    trajectoryList.add(new TrajectorySegment(f.getObjectId(), segment++, currentSpatioTemporalPoints.toArray(new SpatioTemporalPoint[0]), 0, 0, 0, 0, 0, 0));
                    currentSpatioTemporalPoints.clear();
                }
                return trajectoryList.iterator();
            })

            .groupBy(f->f._2.getObjectId()).flatMapToPair(f->{

                List<TrajectorySegment> trSegments = new ArrayList<>();
                f._2.forEach(t->trSegments.add(t._2));
                trSegments.sort(Comparator.comparingLong(seg->seg.getSpatioTemporalPoints()[0].getTimestamp()));

                List<Tuple2<Void, TrajectorySegment>> finalList = new ArrayList<>();
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
                        finalList.add(Tuple2.apply(null,new TrajectorySegment(f._1, ++segmentNum, currentMerged)));
                        currentMerged.clear();
                        currentMerged.add(trSegments.get(i+1));
                    }
                }

                //leftovers
                if(currentMerged.size()>0){
                    finalList.add(Tuple2.apply(null,new TrajectorySegment(f._1,++segmentNum, currentMerged)));
                }
                return finalList.iterator();
            });

            List<Tuple2<Void,TrajectorySegment>> trajs = pairRDDRangeQuery.collect();
//            List<Tuple2<Void,TrajectorySegment>> trajs = new ArrayList<>(pairRDDRangeQuery.collect());
            long num = trajs.size();

            long numOfPoints = 0;
            for (Tuple2<Void, TrajectorySegment> voidTrajectoryTuple2 : trajs) {
                numOfPoints = numOfPoints + voidTrajectoryTuple2._2.getSpatioTemporalPoints().length;
            }

//            trajs.sort(Comparator.comparingLong(seg->seg._2.getSpatioTemporalPoints()[0].getTimestamp()+Long.parseLong(seg._2.getObjectId())));


//            if(trajs.size()==994){
//                int k = 0;
//                for (int ff = 0; ff < trajs.size(); ff++) {
//                    if(trajs.get(ff)._2.getObjectId().equals("226338000") && (trajs.get(ff)._2.getSegment()==2 || trajs.get(ff)._2.getSegment()==3)){
//
//                        System.out.println(trajs.get(ff));
//                    }
//                    bw.write(trajs.get(ff)._2.getObjectId() +" "+ trajs.get(ff)._2.getSegment()+" "+trajs.get(ff)._2.getSpatioTemporalPoints().length + " "+trajs.get(ff)._2.getSpatioTemporalPoints()[0].getTimestamp() +"\n");
//                    k++;
//                }
//                try {
//                    bw.close();
//                    throw new Exception("THERE EXISTS segment with one point" + num +" k is "+k);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
////            System.out.println("END");
//            }

            long endTime = System.currentTimeMillis();
            times.add((endTime - startTime));

            bw.write((endTime - startTime)+";"+num+";"+numOfPoints+";"+ DataPage.counter);
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
