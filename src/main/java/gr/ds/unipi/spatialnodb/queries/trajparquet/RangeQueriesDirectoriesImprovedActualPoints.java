package gr.ds.unipi.spatialnodb.queries.trajparquet;

import com.typesafe.config.Config;
import gr.ds.unipi.spatialnodb.AppConfig;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.SpatioTemporalPoint;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.TrajectorySegment;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.TrajectorySegmentReadSupport;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.column.page.DataPage;
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

public class RangeQueriesDirectoriesImprovedActualPoints {
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

//            FilterPredicate xAxis = and(gtEq(doubleColumn("maxLongitude"), queryMinLongitude), ltEq(doubleColumn("minLongitude"), queryMaxLongitude));
//            FilterPredicate yAxis = and(gtEq(doubleColumn("maxLatitude"), queryMinLatitude), ltEq(doubleColumn("minLatitude"), queryMaxLatitude));
//            FilterPredicate tAxis = and(gtEq(longColumn("maxTimestamp"), queryMinTimestamp), ltEq(longColumn("minTimestamp"), queryMaxTimestamp));

//            ParquetInputFormat.setFilterPredicate(job.getConfiguration(), and(tAxis, and(xAxis, yAxis)));

            long[] hilStart = HilbertUtil.scaleGeoTemporalPoint(queryMinLongitude, minLon, maxLon,queryMinLatitude, minLat, maxLat, queryMinTimestamp, minTime, maxTime, maxOrdinates);
            long[] hilEnd = HilbertUtil.scaleGeoTemporalPoint(queryMaxLongitude, minLon, maxLon, queryMaxLatitude, minLat, maxLat, queryMaxTimestamp, minTime, maxTime, maxOrdinates);
            Ranges ranges = hilbertCurve.query(hilStart, hilEnd, 0);
            StringBuilder sbFullyCovers = new StringBuilder();
            StringBuilder sbIntersected = new StringBuilder();

            ranges.toList().forEach(range -> {
                for (long r = range.low(); r <= range.high(); r++) {
                    if(directoriesSet.contains(String.valueOf(r))) {
                        long[] arr = hilbertCurve.point(r);
                        if (arr[0] == hilStart[0] || arr[1] == hilStart[1] || arr[2] == hilStart[2]
                                || arr[0] == hilEnd[0] || arr[1] == hilEnd[1] || arr[2] == hilEnd[2]) {
                            sbIntersected.append(parquetPath+"/"+r+",");
                        }else{
                            sbFullyCovers.append(parquetPath+"/"+r+",");
                        }
                    }
                }
            });

            if(sbIntersected.length()==0){
                bw.write(0+";"+0+";"+0+";"+0);
                DataPage.counter = 0;
                bw.newLine();
                continue;
            }


            if(sbFullyCovers.length() != 0){
                sbFullyCovers.deleteCharAt(sbFullyCovers.length()-1);
            }
            sbIntersected.deleteCharAt(sbIntersected.length()-1);

//            System.out.println("directoriesSet size"+directoriesSet.size());
//            System.out.println("sb.string "+sb.toString());

            long startTime = System.currentTimeMillis();

            JavaPairRDD<Void, TrajectorySegment> pairRDD = (JavaPairRDD<Void, TrajectorySegment>) jsc.newAPIHadoopFile(sbIntersected.toString()/*parquetPath*/, ParquetInputFormat.class, Void.class, TrajectorySegment.class, job.getConfiguration());
//            JavaPairRDD<Void, TrajectorySegment> pairRDDRangeQuery = pairRDD;

            JavaPairRDD<Void, TrajectorySegment> pairRDDRangeQuery = (JavaPairRDD<Void, TrajectorySegment>) pairRDD

                    .flatMapValues(f -> {

                        List<TrajectorySegment> trajectoryList = new ArrayList<>();
                        List<SpatioTemporalPoint> currentSpatioTemporalPoints = new ArrayList<>();
//                        long segment = 1;
                        long segment = f.getSegment();

                        SpatioTemporalPoint[] spatioTemporalPoints = f.getSpatioTemporalPoints();

                        if(HilbertUtil.inBox(spatioTemporalPoints[0].getLongitude(), spatioTemporalPoints[0].getLatitude(),spatioTemporalPoints[0].getTimestamp(),queryMinLongitude, queryMinLatitude, queryMinTimestamp, queryMaxLongitude, queryMaxLatitude, queryMaxTimestamp)){
                            if(f.getSegment()>1 || f.getSegment()<-1){
                                if(Double.compare(spatioTemporalPoints[0].getLongitude(),queryMinLongitude)!=0 && Double.compare(spatioTemporalPoints[0].getLongitude(),queryMaxLongitude)!=0
                                        && Double.compare(spatioTemporalPoints[0].getLatitude(),queryMinLatitude)!=0 && Double.compare(spatioTemporalPoints[0].getLatitude(),queryMaxLatitude)!=0
                                        && Long.compare(spatioTemporalPoints[0].getTimestamp(),queryMinTimestamp)!=0 && Long.compare(spatioTemporalPoints[0].getTimestamp(),queryMaxTimestamp)!=0){
                                    currentSpatioTemporalPoints.add(spatioTemporalPoints[0]);
                                }
                            }else{
                                currentSpatioTemporalPoints.add(spatioTemporalPoints[0]);
                            }
                        }

                        for (int i = 1; i < spatioTemporalPoints.length-1; i++) {
                            if(HilbertUtil.inBox(spatioTemporalPoints[i].getLongitude(), spatioTemporalPoints[i].getLatitude(),spatioTemporalPoints[i].getTimestamp(),queryMinLongitude, queryMinLatitude, queryMinTimestamp, queryMaxLongitude, queryMaxLatitude, queryMaxTimestamp)){
                                currentSpatioTemporalPoints.add(spatioTemporalPoints[i]);
                            }else{
                                if(currentSpatioTemporalPoints.size()!=0){
                                    trajectoryList.add(new TrajectorySegment(f.getObjectId(), segment/*segment++*/, currentSpatioTemporalPoints.toArray(new SpatioTemporalPoint[0]), 0, 0, 0, 0, 0, 0));
                                    currentSpatioTemporalPoints.clear();
                                }
                            }
                        }

                        if(HilbertUtil.inBox(spatioTemporalPoints[spatioTemporalPoints.length-1].getLongitude(), spatioTemporalPoints[spatioTemporalPoints.length-1].getLatitude(),spatioTemporalPoints[spatioTemporalPoints.length-1].getTimestamp(),queryMinLongitude, queryMinLatitude, queryMinTimestamp, queryMaxLongitude, queryMaxLatitude, queryMaxTimestamp)){
                            if(f.getSegment()>=1){
                                if(Double.compare(spatioTemporalPoints[spatioTemporalPoints.length-1].getLongitude(),queryMinLongitude)!=0 && Double.compare(spatioTemporalPoints[spatioTemporalPoints.length-1].getLongitude(),queryMaxLongitude)!=0
                                        && Double.compare(spatioTemporalPoints[spatioTemporalPoints.length-1].getLatitude(),queryMinLatitude)!=0 && Double.compare(spatioTemporalPoints[spatioTemporalPoints.length-1].getLatitude(),queryMaxLatitude)!=0
                                        && Long.compare(spatioTemporalPoints[spatioTemporalPoints.length-1].getTimestamp(),queryMinTimestamp)!=0 && Long.compare(spatioTemporalPoints[spatioTemporalPoints.length-1].getTimestamp(),queryMaxTimestamp)!=0){
                                    currentSpatioTemporalPoints.add(spatioTemporalPoints[spatioTemporalPoints.length-1]);
                                }
                            }else{
                                currentSpatioTemporalPoints.add(spatioTemporalPoints[spatioTemporalPoints.length-1]);
                            }
                        }

                        if (currentSpatioTemporalPoints.size() > 0) {
                            trajectoryList.add(new TrajectorySegment(f.getObjectId(), segment/*segment++*/, currentSpatioTemporalPoints.toArray(new SpatioTemporalPoint[0]), 0, 0, 0, 0, 0, 0));
                            currentSpatioTemporalPoints.clear();
                        }
                        return trajectoryList.iterator();
                    });

            if(sbFullyCovers.length()!=0){
                pairRDDRangeQuery = pairRDDRangeQuery.union((JavaPairRDD<Void, TrajectorySegment>)jsc.newAPIHadoopFile(sbFullyCovers.toString(), ParquetInputFormat.class, Void.class, TrajectorySegment.class, job.getConfiguration()));
            }

            pairRDDRangeQuery = pairRDDRangeQuery.groupBy(f->f._2.getObjectId()).flatMapToPair(f->{

                        List<TrajectorySegment> trSegments = new ArrayList<>();
                        f._2.forEach(t->trSegments.add(t._2));

//                        Comparator<TrajectorySegment> comparator = Comparator.comparingLong(d-> d.getSpatioTemporalPoints()[0].getTimestamp());
//                        //the second comparator is not really needed, but it can handle the intersected points of lines with cubes that have the same timestamp.
////                        comparator = comparator.thenComparingLong(d-> d.getSpatioTemporalPoints()[1].getTimestamp());
//                        comparator = comparator.thenComparingLong(d -> {
//                            if(d.getSpatioTemporalPoints().length==1){
//                                return (-1)*(d.getSpatioTemporalPoints().length);
//                            }
//                            return d.getSpatioTemporalPoints()[1].getTimestamp();
//                        });
//                        //the third comparator is not really needed, but it can handle erroneous data sets in terms of containing more than one points of a object id with the same timestamp but with different location
//                        comparator = comparator.thenComparingDouble(d-> d.getSpatioTemporalPoints()[0].getLongitude());

                        Comparator<TrajectorySegment> comparator = Comparator.comparingLong(d-> d.getSpatioTemporalPoints()[0].getTimestamp());
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
                    }).filter(f->{if(f._2.getSpatioTemporalPoints().length==0) {return false;} return true;});

            List<Tuple2<Void,TrajectorySegment>> trajs = pairRDDRangeQuery.collect();
            long num = trajs.size();

            long numOfPoints = 0;
            for (Tuple2<Void, TrajectorySegment> voidTrajectoryTuple2 : trajs) {
                numOfPoints = numOfPoints + voidTrajectoryTuple2._2.getSpatioTemporalPoints().length;
//                System.out.println(voidTrajectoryTuple2._2.getObjectId()+" "+voidTrajectoryTuple2._2.getSpatioTemporalPoints().length+" "+Arrays.toString(voidTrajectoryTuple2._2.getSpatioTemporalPoints()));
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
