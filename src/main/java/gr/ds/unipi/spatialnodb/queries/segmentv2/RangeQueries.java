package gr.ds.unipi.spatialnodb.queries.segmentv2;

import com.typesafe.config.Config;
import gr.ds.unipi.spatialnodb.AppConfig;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.messages.common.segmentv2.SpatioTemporalPoints;
import gr.ds.unipi.spatialnodb.messages.common.segmentv2.TrajectorySegment;
import gr.ds.unipi.spatialnodb.messages.common.segmentv2.TrajectorySegmentReadSupport;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.apache.parquet.filter2.predicate.FilterApi.*;

public class RangeQueries {
    public static void main(String args[]) throws IOException {

        Config config = AppConfig.newAppConfig(args[0]/*"src/main/resources/app-new.conf"*/).getConfig();

        Config dataLoading = config.getConfig("queries");
        final String parquetPath = dataLoading.getString("parquetPath");
        final String queriesFilePath = dataLoading.getString("queriesFilePath");
        final String queriesFileExport = dataLoading.getString("queriesFileExport");
        Job job = Job.getInstance();

        ParquetInputFormat.setReadSupportClass(job, TrajectorySegmentReadSupport.class);

        SparkConf sparkConf = new SparkConf();//.registerKryoClasses(new Class[]{SpatioTemporalPoints.class});/*.setMaster("local[1]").set("spark.executor.memory","1g")*/
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

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

            ParquetInputFormat.setFilterPredicate(job.getConfiguration(), and(tAxis, and(xAxis, yAxis)));
            JavaPairRDD<Void, TrajectorySegment> pairRDD = (JavaPairRDD<Void, TrajectorySegment>) jsc.newAPIHadoopFile(parquetPath, ParquetInputFormat.class, Void.class, TrajectorySegment.class, job.getConfiguration());
            long startTime = System.currentTimeMillis();
            JavaPairRDD<Void, TrajectorySegment> pairRDDRangeQuery = (JavaPairRDD<Void, TrajectorySegment>) pairRDD
                    .flatMapValues(f -> {

                List<TrajectorySegment> trajectoryList = new ArrayList<>();
                SpatioTemporalPoints.Builder currentSpatioTemporalPoints = SpatioTemporalPoints.newBuilder();
                long segment = f.getSegment();

                SpatioTemporalPoints spatioTemporalPoints = f.getSpatioTemporalPoints();
                for (int i = 0; i < spatioTemporalPoints.getSptCount() - 1; i++) {
                    //if intersected in time
                    if (spatioTemporalPoints.getSpt(i + 1).getTimestamp() >= queryMinTimestamp && spatioTemporalPoints.getSpt(i).getTimestamp() <= queryMaxTimestamp) {

                        //if one of the points of the line is inside the spatial part of the query
                        if (HilbertUtil.pointInRectangle(spatioTemporalPoints.getSpt(i).getLongitude(), spatioTemporalPoints.getSpt(i).getLatitude(), queryMinLongitude, queryMinLatitude, queryMaxLongitude, queryMaxLatitude)
                                || HilbertUtil.pointInRectangle(spatioTemporalPoints.getSpt(i+1).getLongitude(), spatioTemporalPoints.getSpt(i+1).getLatitude(), queryMinLongitude, queryMinLatitude, queryMaxLongitude, queryMaxLatitude)) {

                            if (currentSpatioTemporalPoints.getSptCount() == 0) {
                                currentSpatioTemporalPoints.addSpt(spatioTemporalPoints.getSpt(i));
                                currentSpatioTemporalPoints.addSpt(spatioTemporalPoints.getSpt(i+1));

                            } else {
                                currentSpatioTemporalPoints.addSpt(spatioTemporalPoints.getSpt(i+1));
                            }

                        } else if (HilbertUtil.lineLineIntersection(spatioTemporalPoints.getSpt(i).getLongitude(), spatioTemporalPoints.getSpt(i).getLatitude(), spatioTemporalPoints.getSpt(i+1).getLongitude(), spatioTemporalPoints.getSpt(i+1).getLatitude(), queryMinLongitude, queryMinLatitude, queryMaxLongitude, queryMinLatitude, true)
                                || HilbertUtil.lineLineIntersection(spatioTemporalPoints.getSpt(i).getLongitude(), spatioTemporalPoints.getSpt(i).getLatitude(), spatioTemporalPoints.getSpt(i+1).getLongitude(), spatioTemporalPoints.getSpt(i+1).getLatitude(), queryMinLongitude, queryMinLatitude, queryMinLongitude, queryMaxLatitude, true)
                                || HilbertUtil.lineLineIntersection(spatioTemporalPoints.getSpt(i).getLongitude(), spatioTemporalPoints.getSpt(i).getLatitude(), spatioTemporalPoints.getSpt(i+1).getLongitude(), spatioTemporalPoints.getSpt(i+1).getLatitude(), queryMinLongitude, queryMaxLatitude, queryMaxLongitude, queryMaxLatitude, false)
                                || HilbertUtil.lineLineIntersection(spatioTemporalPoints.getSpt(i).getLongitude(), spatioTemporalPoints.getSpt(i).getLatitude(), spatioTemporalPoints.getSpt(i+1).getLongitude(), spatioTemporalPoints.getSpt(i+1).getLatitude(), queryMaxLongitude, queryMinLatitude, queryMaxLongitude, queryMaxLatitude, false)) {
                            //if the line penetrates the spatial part of the query
                            if (currentSpatioTemporalPoints.getSptCount() == 0) {
                                currentSpatioTemporalPoints.addSpt(spatioTemporalPoints.getSpt(i));
                                currentSpatioTemporalPoints.addSpt(spatioTemporalPoints.getSpt(i+1));

                            } else {
                                currentSpatioTemporalPoints.addSpt(spatioTemporalPoints.getSpt(i+1));
                            }
//                            System.out.println("HERE "+ spatioTemporalPoints.getSpt(i).getLongitude()+" "+spatioTemporalPoints.getSpt(i).getLatitude() +" "+spatioTemporalPoints[i+1].getLongitude() +" "+spatioTemporalPoints[i+1].getLatitude() +" Query "+ queryMinLongitude+" "+ queryMinLatitude+" "+queryMaxLongitude+" "+ queryMaxLatitude);
                        } else {//if the line does not intersect with the spatial part of the query
                            if (currentSpatioTemporalPoints.getSptCount() > 0) {
                                trajectoryList.add(new TrajectorySegment(f.getObjectId(), segment, currentSpatioTemporalPoints.build(), 0, 0, 0, 0, 0, 0));
                                currentSpatioTemporalPoints.clear();
                            }
                        }
                    }

                }
                if (currentSpatioTemporalPoints.getSptCount() > 0) {
                    trajectoryList.add(new TrajectorySegment(f.getObjectId(), segment, currentSpatioTemporalPoints.build(), 0, 0, 0, 0, 0, 0));
                    currentSpatioTemporalPoints.clear();
                }
                return trajectoryList.iterator();
            })

            .groupBy(f->f._2.getObjectId()).flatMapToPair(f->{

                List<TrajectorySegment> trSegments = new ArrayList<>();
                f._2.forEach(t->trSegments.add(t._2));

                Comparator<TrajectorySegment> comparator = Comparator.comparingLong(d-> d.getSpatioTemporalPoints().getSpt(0).getTimestamp());
                comparator = comparator.thenComparingLong(d-> Math.abs(d.getSegment()));
                trSegments.sort(comparator);

                List<Tuple2<Void, TrajectorySegment>> finalList = new ArrayList<>();
                List<TrajectorySegment> currentMerged = new ArrayList<>();
                currentMerged.add(trSegments.get(0));

                int segmentNum = 0;

                for (int i = 0; i < trSegments.size()-1; i++) {
                    SpatioTemporalPoints.SpatioTemporalPoint spatioTemporalPoint1 = trSegments.get(i).getSpatioTemporalPoints().getSpt(trSegments.get(i).getSpatioTemporalPoints().getSptCount()-1);
                    SpatioTemporalPoints.SpatioTemporalPoint spatioTemporalPoint2 = trSegments.get(i+1).getSpatioTemporalPoints().getSpt(0);

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

            long num = pairRDDRangeQuery.count();
            long endTime = System.currentTimeMillis();
            times.add((endTime - startTime));

            long numOfPoints = 0;
            for (Tuple2<Void, gr.ds.unipi.spatialnodb.messages.common.segmentv2.TrajectorySegment> voidTrajectoryTuple2 : pairRDDRangeQuery.collect()) {
                numOfPoints = numOfPoints + voidTrajectoryTuple2._2.getSpatioTemporalPoints().getSptCount();
            }
            bw.write((endTime - startTime)+";"+num+";"+numOfPoints+";"+ DataPage.counter);
            DataPage.counter = 0;
            bw.newLine();
//            pairRDDRangeQuery.collect().forEach(g-> System.out.println(g._2.toString()));
//            System.out.println("Counted: "+ pairRDDRangeQuery.count());
        }
        for (int ind = 0; ind < 10; ind++) {
            times.remove(0);
        }
        //System.out.println("Average Execution Time: "+times.stream().mapToLong(Long::longValue).average().getAsDouble());
        bw.write(times.stream().mapToLong(Long::longValue).average().getAsDouble()+"");

        bw.close();
        br.close();
    }
}
