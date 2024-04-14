package gr.ds.unipi.spatialnodb.queries.trajparquetold;

import com.typesafe.config.Config;
import gr.ds.unipi.spatialnodb.AppConfig;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.messages.common.trajparquetold.SpatioTemporalPoint;
import gr.ds.unipi.spatialnodb.messages.common.trajparquetold.TrajectorySegment;
import gr.ds.unipi.spatialnodb.messages.common.trajparquetold.TrajectorySegmentReadSupport;
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
import java.util.*;

import static org.apache.parquet.filter2.predicate.FilterApi.*;

public class RangeQueriesActualPoints {
    public static void main(String args[]) throws IOException {

        Config config = AppConfig.newAppConfig(args[0]/*"src/main/resources/app-new.conf"*/).getConfig();

        Config dataLoading = config.getConfig("queries");
        final String parquetPath = dataLoading.getString("parquetPath");
        final String queriesFilePath = dataLoading.getString("queriesFilePath");
        final String queriesFileExport = dataLoading.getString("queriesFileExport");

        Job job = Job.getInstance();

        ParquetInputFormat.setReadSupportClass(job, TrajectorySegmentReadSupport.class);

        SparkConf sparkConf = new SparkConf();//.registerKryoClasses(new Class[]{SpatioTemporalPoint.class,SpatioTemporalPoint[].class});/*.setMaster("local[1]").set("spark.executor.memory","1g")*/
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
            long startTime = System.currentTimeMillis();

            JavaPairRDD<Void, TrajectorySegment> pairRDD = (JavaPairRDD<Void, TrajectorySegment>) jsc.newAPIHadoopFile(parquetPath, ParquetInputFormat.class, Void.class, TrajectorySegment.class, job.getConfiguration());
//            JavaPairRDD<Void, TrajectorySegment> pairRDDRangeQuery = pairRDD;
            JavaPairRDD<Void, TrajectorySegment> pairRDDRangeQuery = (JavaPairRDD<Void, TrajectorySegment>) pairRDD
                    .flatMapValues(f -> {

                        List<TrajectorySegment> trajectoryList = new ArrayList<>();
                        List<SpatioTemporalPoint> currentSpatioTemporalPoints = new ArrayList<>();
//                        long segment = 1;
                        long segment = f.getSegment();

                        SpatioTemporalPoint[] spatioTemporalPoints = f.getSpatioTemporalPoints();
                        for (int i = 0; i < spatioTemporalPoints.length; i++) {

                            if(HilbertUtil.inBox(spatioTemporalPoints[i].getLongitude(), spatioTemporalPoints[i].getLatitude(),spatioTemporalPoints[i].getTimestamp(),queryMinLongitude, queryMinLatitude, queryMinTimestamp, queryMaxLongitude, queryMaxLatitude, queryMaxTimestamp)){
                                currentSpatioTemporalPoints.add(spatioTemporalPoints[i]);
                            }else{
                                if(currentSpatioTemporalPoints.size()!=0){
                                    trajectoryList.add(new TrajectorySegment(f.getObjectId(), segment, currentSpatioTemporalPoints.toArray(new SpatioTemporalPoint[0]), 0, 0, 0, 0, 0, 0));
                                    currentSpatioTemporalPoints.clear();
                                }
                            }

                        }
                        if (currentSpatioTemporalPoints.size() > 0) {
                            trajectoryList.add(new TrajectorySegment(f.getObjectId(), segment, currentSpatioTemporalPoints.toArray(new SpatioTemporalPoint[0]), 0, 0, 0, 0, 0, 0));
                            currentSpatioTemporalPoints.clear();
                        }
                        return trajectoryList.iterator();
                    })

                    .groupBy(f->f._2.getObjectId()).flatMapToPair(f->{

                        List<TrajectorySegment> trSegments = new ArrayList<>();
                        f._2.forEach(t->trSegments.add(t._2));

                        Comparator<TrajectorySegment> comparator = Comparator.comparingLong(d -> d.getSpatioTemporalPoints()[0].getTimestamp());
//                        comparator = comparator.thenComparingLong(d -> {
//                            if(d.getSpatioTemporalPoints().length==1){
//                                return (-1)*(d.getSpatioTemporalPoints().length);
//                            }
//                            return d.getSpatioTemporalPoints()[1].getTimestamp();
//                        });
//                        comparator = comparator.thenComparingDouble(d -> d.getSpatioTemporalPoints()[0].getLongitude());
                        comparator = comparator.thenComparingLong(d-> Math.abs(d.getSegment()));

                        trSegments.sort(comparator);


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
            long num = trajs.size();

            long numOfPoints = 0;
            for (Tuple2<Void, TrajectorySegment> voidTrajectoryTuple2 : trajs) {
                numOfPoints = numOfPoints + voidTrajectoryTuple2._2.getSpatioTemporalPoints().length;
//                System.out.println(voidTrajectoryTuple2._2.getObjectId()+" "+voidTrajectoryTuple2._2.getSpatioTemporalPoints().length+" "+ Arrays.toString(voidTrajectoryTuple2._2.getSpatioTemporalPoints()));
            }

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
