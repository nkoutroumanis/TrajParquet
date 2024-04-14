package gr.ds.unipi.spatialnodb.queries.geoparquet;

import com.typesafe.config.Config;
import gr.ds.unipi.spatialnodb.AppConfig;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.messages.common.geoparquet.Trajectory;
import gr.ds.unipi.spatialnodb.messages.common.geoparquet.TrajectoryReadSupport;
import gr.ds.unipi.spatialnodb.messages.common.trajparquetold.TrajectorySegment;
import gr.ds.unipi.spatialnodb.shapes.STPoint;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import scala.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static org.apache.parquet.filter2.predicate.FilterApi.*;

public class RangeQueries {
    public static void main(String args[]) throws IOException {

        Config config = AppConfig.newAppConfig(args[0]/*"src/main/resources/app-new.conf"*/).getConfig();

        Config dataLoading = config.getConfig("queries");
        final String parquetPath = dataLoading.getString("parquetPath");
        final String queriesFilePath = dataLoading.getString("queriesFilePath");
        final String queriesFileExport = dataLoading.getString("queriesFileExport");

        Job job = Job.getInstance();

        ParquetInputFormat.setReadSupportClass(job, TrajectoryReadSupport.class);

        SparkConf sparkConf = new SparkConf();/*.setMaster("local[1]").set("spark.executor.memory","1g")*/
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

            FilterPredicate xAxis= and(gtEq(doubleColumn("maxLongitude"), queryMinLongitude), ltEq(doubleColumn("minLongitude"), queryMaxLongitude));
            FilterPredicate yAxis= and(gtEq(doubleColumn("maxLatitude"), queryMinLatitude), ltEq(doubleColumn("minLatitude"), queryMaxLatitude));
            FilterPredicate tAxis= and(gtEq(longColumn("maxTimestamp"), queryMinTimestamp), ltEq(longColumn("minTimestamp"), queryMaxTimestamp));

            ParquetInputFormat.setFilterPredicate(job.getConfiguration(), and(tAxis, and(xAxis, yAxis)));
            long startTime = System.currentTimeMillis();

            JavaPairRDD<Void, Trajectory> pairRDD = (JavaPairRDD<Void, Trajectory>) jsc.newAPIHadoopFile(parquetPath,ParquetInputFormat.class, Void.class, Trajectory.class,job.getConfiguration());
//            JavaPairRDD<Void,Trajectory> pairRDDRangeQuery = pairRDD;

            JavaPairRDD<Void, Trajectory> pairRDDRangeQuery = (JavaPairRDD<Void, Trajectory>) pairRDD.flatMapValues(f->{

                List<Trajectory> trajectoryList = new ArrayList<>();
                List<Coordinate> currentCoordinates = new ArrayList<>();
                List<Long> currentTimestamps =new ArrayList<>();
                long segment = f.getTrajectoryId();

                Coordinate[] coordinates = f.getLineString().getCoordinates();
                for (int i = 0; i < coordinates.length-1; i++) {

                    Optional<STPoint[]> stPoints = HilbertUtil.liangBarsky(coordinates[i].x, coordinates[i].y,f.getTimestamps()[i],
                            coordinates[i+1].x, coordinates[i+1].y,f.getTimestamps()[i+1]
                            ,queryMinLongitude, queryMinLatitude, queryMinTimestamp, queryMaxLongitude, queryMaxLatitude, queryMaxTimestamp);


                    if(stPoints.isPresent()){
                        if(stPoints.get().length==2){
                            if(stPoints.get()[0].getT() == f.getTimestamps()[i] &&
                                    stPoints.get()[1].getT() == f.getTimestamps()[i+1]){
//                                    if(stPoints.get()[0].equals(new STPoint(spatioTemporalPoints[i].getLongitude(),spatioTemporalPoints[i].getLatitude(),spatioTemporalPoints[i].getTimestamp())) &&
//                                            stPoints.get()[1].equals(new STPoint(spatioTemporalPoints[i+1].getLongitude(),spatioTemporalPoints[i+1].getLatitude(),spatioTemporalPoints[i+1].getTimestamp()))){
                                if(currentCoordinates.size()!=0){
                                    if(!(currentCoordinates.get(currentCoordinates.size()-1).equals(coordinates[i])
                                        && currentTimestamps.get(currentTimestamps.size()-1).equals(f.getTimestamps()[i]))){
                                        throw new Exception("The i th element of the segment should be the last point of the current list.");
                                    }
                                }

                                if (currentCoordinates.size() == 0) {
                                    currentCoordinates.add(new Coordinate(coordinates[i].x,coordinates[i].y));
                                    currentTimestamps.add(f.getTimestamps()[i]);
                                }
                                currentCoordinates.add(new Coordinate(coordinates[i+1].x,coordinates[i+1].y));
                                currentTimestamps.add(f.getTimestamps()[i+1]);
                            }else if(stPoints.get()[0].getT() == f.getTimestamps()[i]){
//                                    }else if(stPoints.get()[0].equals(new STPoint(spatioTemporalPoints[i].getLongitude(),spatioTemporalPoints[i].getLatitude(),spatioTemporalPoints[i].getTimestamp()))){

                                if (currentCoordinates.size() == 0) {
                                    currentCoordinates.add(new Coordinate(coordinates[i].x,coordinates[i].y));
                                    currentTimestamps.add(f.getTimestamps()[i]);
                                }

                                currentCoordinates.add(new Coordinate(stPoints.get()[1].getX(), stPoints.get()[1].getY()));
                                currentTimestamps.add(stPoints.get()[1].getT());

                                trajectoryList.add(new Trajectory(f.getObjectId(), segment, new GeometryFactory().createLineString(currentCoordinates.toArray(new Coordinate[0])), currentTimestamps.stream().mapToLong(l->l).toArray(),0,0,0,0,0,0 ));
                                currentCoordinates.clear();
                                currentTimestamps.clear();
                            }else if(stPoints.get()[1].getT() == f.getTimestamps()[i+1]){
//                                    }else if(stPoints.get()[1].equals(new STPoint(spatioTemporalPoints[i+1].getLongitude(),spatioTemporalPoints[i+1].getLatitude(),spatioTemporalPoints[i+1].getTimestamp()))){

                                if(currentCoordinates.size()==1){
                                    throw new Exception("Exception for the current list, it will be flushed and has only one element.");
                                }

                                if (currentCoordinates.size() != 0) {
                                    trajectoryList.add(new Trajectory(f.getObjectId(), segment, new GeometryFactory().createLineString(currentCoordinates.toArray(new Coordinate[0])), currentTimestamps.stream().mapToLong(l->l).toArray(),0,0,0,0,0,0 ));
                                    currentCoordinates.clear();
                                    currentTimestamps.clear();
                                }
                                currentCoordinates.add(new Coordinate(stPoints.get()[0].getX(), stPoints.get()[0].getY()));
                                currentTimestamps.add( stPoints.get()[0].getT());

                                currentCoordinates.add(new Coordinate(coordinates[i+1].x,coordinates[i+1].y));
                                currentTimestamps.add(f.getTimestamps()[i+1]);

                            }else{
                                if(currentCoordinates.size()!=0){
                                    throw new Exception("The current list has elements while it should not have.");
                                }

                                currentCoordinates.add(new Coordinate(stPoints.get()[0].getX(), stPoints.get()[0].getY()));
                                currentTimestamps.add( stPoints.get()[0].getT());

                                currentCoordinates.add(new Coordinate(stPoints.get()[1].getX(), stPoints.get()[1].getY()));
                                currentTimestamps.add(stPoints.get()[1].getT());

                                trajectoryList.add(new Trajectory(f.getObjectId(), segment, new GeometryFactory().createLineString(currentCoordinates.toArray(new Coordinate[0])), currentTimestamps.stream().mapToLong(l->l).toArray(),0,0,0,0,0,0 ));
                                currentCoordinates.clear();
                                currentTimestamps.clear();
                            }
                        }else{
                            throw new Exception("The array from the Liang Barsky should contain at least one element");
                        }
                    }else{
                        if (currentCoordinates.size() > 0) {
                            trajectoryList.add(new Trajectory(f.getObjectId(), segment, new GeometryFactory().createLineString(currentCoordinates.toArray(new Coordinate[0])), currentTimestamps.stream().mapToLong(l->l).toArray(),0,0,0,0,0,0 ));
                            currentCoordinates.clear();
                            currentTimestamps.clear();
                        }
                    }
                }
                if (currentCoordinates.size() > 0) {
                    trajectoryList.add(new Trajectory(f.getObjectId(), segment, new GeometryFactory().createLineString(currentCoordinates.toArray(new Coordinate[0])), currentTimestamps.stream().mapToLong(l->l).toArray(),0,0,0,0,0,0 ));
                    currentCoordinates.clear();
                    currentTimestamps.clear();
                }

                return trajectoryList.iterator();

            }).groupBy(f->f._2.getObjectId()).flatMapToPair(f->{

                List<Trajectory> trSegments = new ArrayList<>();
                f._2.forEach(t->trSegments.add(t._2));

                Comparator<Trajectory> comparator = Comparator.comparingLong(d-> d.getTimestamps()[0]);
                comparator = comparator.thenComparingLong(d-> Math.abs(d.getTrajectoryId()));
                trSegments.sort(comparator);

                List<Tuple2<Void, Trajectory>> finalList = new ArrayList<>();
                List<Trajectory> currentMerged = new ArrayList<>();
                currentMerged.add(trSegments.get(0));

                int segmentNum = 0;

                for (int i = 0; i < trSegments.size()-1; i++) {

                    long timestamp1 = trSegments.get(i).getTimestamps()[trSegments.get(i).getTimestamps().length-1];
                    long timestamp2 = trSegments.get(i+1).getTimestamps()[0];

                    Coordinate coordinate1 =  trSegments.get(i).getLineString().getCoordinates()[trSegments.get(i).getLineString().getCoordinates().length-1];
                    Coordinate coordinate2 =  trSegments.get(i+1).getLineString().getCoordinates()[0];

                    if(coordinate1.equals(coordinate2) && timestamp1==timestamp2){
                        currentMerged.add(trSegments.get(i+1));
                    }else{
                        //clean currentMerged and add to the final list
                        finalList.add(Tuple2.apply(null,new Trajectory(f._1, ++segmentNum, currentMerged)));
                        currentMerged.clear();
                        currentMerged.add(trSegments.get(i+1));
                    }
                }

                //leftovers
                if(currentMerged.size()>0){
                    finalList.add(Tuple2.apply(null,new Trajectory(f._1,++segmentNum, currentMerged)));
                }
                return finalList.iterator();
            });

            List<Tuple2<Void, Trajectory>> trajs = pairRDDRangeQuery.collect();
            long num = trajs.size();

            long numOfPoints = 0;
            for (Tuple2<Void, Trajectory> voidTrajectoryTuple2 : trajs) {
                numOfPoints = numOfPoints + voidTrajectoryTuple2._2.getTimestamps().length;
            }

            long endTime = System.currentTimeMillis();
            times.add((endTime - startTime));

            bw.write((endTime - startTime)+";"+num+";"+numOfPoints+";"+DataPage.counter);
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
