package gr.ds.unipi.spatialnodb.queries.segmentv3;

import com.mongodb.client.model.geojson.LineString;
import com.mongodb.client.model.geojson.Position;
import com.typesafe.config.Config;
import gr.ds.unipi.spatialnodb.AppConfig;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.messages.common.segmentv3.Trajectory;
import gr.ds.unipi.spatialnodb.messages.common.segmentv3.TrajectoryReadSupport;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.BsonArray;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonValue;
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
            JavaPairRDD<Void, Trajectory> pairRDD = (JavaPairRDD<Void, Trajectory>) jsc.newAPIHadoopFile(parquetPath,ParquetInputFormat.class, Void.class, Trajectory.class,job.getConfiguration());
            long startTime = System.currentTimeMillis();

            JavaPairRDD<Void, Trajectory> pairRDDRangeQuery = (JavaPairRDD<Void, Trajectory>) pairRDD.flatMapValues(f->{

                List<Trajectory> trajectoryList = new ArrayList<>();
                List<Position> currentCoordinates = new ArrayList<>();
                List<Long> currentTimestamps =new ArrayList<>();
                long part = 1;

                BsonArray coordinates = f.getLineString().toBsonDocument().getArray("coordinates");
                for (int i = 0; i < coordinates.size()-1; i++) {
                    
                    coordinates.get(i).asArray().get(0).asDouble().doubleValue();
                            
                    //if intersected in time
                    if(f.getTimestamps()[i+1]>= queryMinTimestamp && f.getTimestamps()[i]<= queryMaxTimestamp){

                        //if one of the points of the line is inside the spatial part of the query
                        if(HilbertUtil.pointInRectangle(coordinates.get(i).asArray().get(0).asDouble().doubleValue(),coordinates.get(i).asArray().get(1).asDouble().doubleValue(), queryMinLongitude, queryMinLatitude, queryMaxLongitude, queryMaxLatitude)
                                || HilbertUtil.pointInRectangle(coordinates.get(i+1).asArray().get(0).asDouble().doubleValue(),coordinates.get(i+1).asArray().get(1).asDouble().doubleValue(), queryMinLongitude, queryMinLatitude, queryMaxLongitude, queryMaxLatitude)){

                            if (currentCoordinates.size() == 0) {
                                currentCoordinates.add(new Position(coordinates.get(i).asArray().get(0).asDouble().doubleValue(),coordinates.get(i).asArray().get(1).asDouble().doubleValue()));
                                currentCoordinates.add(new Position(coordinates.get(i+1).asArray().get(0).asDouble().doubleValue(),coordinates.get(i+1).asArray().get(1).asDouble().doubleValue()));
                                currentTimestamps.add(f.getTimestamps()[i]);
                                currentTimestamps.add(f.getTimestamps()[i+1]);

                            }else{
                                currentCoordinates.add(new Position(coordinates.get(i+1).asArray().get(0).asDouble().doubleValue(),coordinates.get(i+1).asArray().get(1).asDouble().doubleValue()));
                                currentTimestamps.add(f.getTimestamps()[i+1]);
                            }

                        }else if(HilbertUtil.lineLineIntersection(coordinates.get(i).asArray().get(0).asDouble().doubleValue(),coordinates.get(i).asArray().get(1).asDouble().doubleValue(),coordinates.get(i+1).asArray().get(0).asDouble().doubleValue(),coordinates.get(i+1).asArray().get(1).asDouble().doubleValue(),queryMinLongitude, queryMinLatitude,queryMaxLongitude, queryMinLatitude,true)
                        || HilbertUtil.lineLineIntersection(coordinates.get(i).asArray().get(0).asDouble().doubleValue(),coordinates.get(i).asArray().get(1).asDouble().doubleValue(),coordinates.get(i+1).asArray().get(0).asDouble().doubleValue(),coordinates.get(i+1).asArray().get(1).asDouble().doubleValue(),queryMinLongitude, queryMinLatitude,queryMinLongitude, queryMaxLatitude,true)
                        || HilbertUtil.lineLineIntersection(coordinates.get(i).asArray().get(0).asDouble().doubleValue(),coordinates.get(i).asArray().get(1).asDouble().doubleValue(),coordinates.get(i+1).asArray().get(0).asDouble().doubleValue(),coordinates.get(i+1).asArray().get(1).asDouble().doubleValue(),queryMinLongitude, queryMaxLatitude,queryMaxLongitude,queryMaxLatitude,false)
                        || HilbertUtil.lineLineIntersection(coordinates.get(i).asArray().get(0).asDouble().doubleValue(),coordinates.get(i).asArray().get(1).asDouble().doubleValue(),coordinates.get(i+1).asArray().get(0).asDouble().doubleValue(),coordinates.get(i+1).asArray().get(1).asDouble().doubleValue(),queryMaxLongitude, queryMinLatitude,queryMaxLongitude,queryMaxLatitude,false)){
                            //if the line penetrates the spatial part of the query
                            if (currentCoordinates.size() == 0) {
                                currentCoordinates.add(new Position(coordinates.get(i).asArray().get(0).asDouble().doubleValue(),coordinates.get(i).asArray().get(1).asDouble().doubleValue()));
                                currentCoordinates.add(new Position(coordinates.get(i+1).asArray().get(0).asDouble().doubleValue(),coordinates.get(i+1).asArray().get(1).asDouble().doubleValue()));
                                currentTimestamps.add(f.getTimestamps()[i]);
                                currentTimestamps.add(f.getTimestamps()[i+1]);

                            }else{
                                currentCoordinates.add(new Position(coordinates.get(i+1).asArray().get(0).asDouble().doubleValue(),coordinates.get(i+1).asArray().get(1).asDouble().doubleValue()));
                                currentTimestamps.add(f.getTimestamps()[i+1]);
                            }
//                            System.out.println("HERE "+ coordinates.get(i).asArray().get(0).asDouble().doubleValue()+" "+coordinates.get(i).asArray().get(1).asDouble().doubleValue() +" "+coordinates.get(i+1).asArray().get(0).asDouble().doubleValue() +" "+coordinates.get(i+1).asArray().get(1).asDouble().doubleValue() +" Query "+ queryMinLongitude+" "+ queryMinLatitude+" "+queryMaxLongitude+" "+ queryMaxLatitude);
                        }else{//if the line does not intersect with the spatial part of the query
                            if (currentCoordinates.size() > 0) {
//                                if(currentCoordinates.size()!=currentTimestamps.size()){
//                                    throw new Exception("DIFF SIZE");
//                                }

                                trajectoryList.add(new Trajectory(f.getObjectId(), part++, BsonDocumentWrapper.parse(new LineString(currentCoordinates).toJson()), currentTimestamps.stream().mapToLong(l->l).toArray(),0,0,0,0,0,0 ));
                                currentCoordinates.clear();
                                currentTimestamps.clear();
                            }
                        }
                    }

                }
                if (currentCoordinates.size() > 0) {
//                    if(currentCoordinates.size()!=currentTimestamps.size()){
//                        throw new Exception("DIFF SIZE");
//                    }
                    trajectoryList.add(new Trajectory(f.getObjectId(), part++, BsonDocumentWrapper.parse(new LineString(currentCoordinates).toJson()), currentTimestamps.stream().mapToLong(l->l).toArray(),0,0,0,0,0,0 ));
                    currentCoordinates.clear();
                    currentTimestamps.clear();
                }

                return trajectoryList.iterator();

            }).groupBy(f->f._2.getObjectId()).flatMapToPair(f->{

                List<Trajectory> trSegments = new ArrayList<>();
                f._2.forEach(t->trSegments.add(t._2));
                trSegments.sort(Comparator.comparingLong(seg->seg.getTimestamps()[0]));

                List<Tuple2<Void, Trajectory>> finalList = new ArrayList<>();
                List<Trajectory> currentMerged = new ArrayList<>();
                currentMerged.add(trSegments.get(0));

                int segmentNum = 0;

                for (int i = 0; i < trSegments.size()-1; i++) {


                    long timestamp1 = trSegments.get(i).getTimestamps()[trSegments.get(i).getTimestamps().length-1];
                    long timestamp2 = trSegments.get(i+1).getTimestamps()[0];

                    BsonValue coordinate1 =  trSegments.get(i).getLineString().toBsonDocument().get("coordinates").asArray().get(trSegments.get(i).getLineString().toBsonDocument().get("coordinates").asArray().size()-1);
                    BsonValue coordinate2 =  trSegments.get(i+1).getLineString().toBsonDocument().get("coordinates").asArray().get(0);

                    if(timestamp1==timestamp2 && Double.compare(coordinate1.asArray().get(0).asDouble().doubleValue(),coordinate2.asArray().get(0).asDouble().doubleValue())==0 && Double.compare(coordinate1.asArray().get(1).asDouble().doubleValue(),coordinate2.asArray().get(1).asDouble().doubleValue())==0){
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

            long num = pairRDDRangeQuery.count();
            long endTime = System.currentTimeMillis();
            times.add((endTime - startTime));

            long numOfPoints = 0;
            for (Tuple2<Void, Trajectory> voidTrajectoryTuple2 : pairRDDRangeQuery.collect()) {
                numOfPoints = numOfPoints + voidTrajectoryTuple2._2.getTimestamps().length;
            }
            bw.write((endTime - startTime)+";"+num+";"+numOfPoints+";"+ DataPage.counter);
            DataPage.counter = 0;
            bw.newLine();
//            pairRDDRangeQuery.collect().forEach(g-> System.out.println(g._2.toString()));
//            System.out.println("Counted: "+ pairRDDRangeQuery.count());
        }
        bw.close();
        br.close();
        times.remove(0);
        System.out.println("Average Execution Time: "+times.stream().mapToLong(Long::longValue).average().getAsDouble());

    }
}
