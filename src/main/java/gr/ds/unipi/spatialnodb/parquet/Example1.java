package gr.ds.unipi.spatialnodb.parquet;

import gr.ds.unipi.spatialnodb.messages.proto.spatialparquet.Geometry;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.apache.parquet.proto.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Random;

import static org.apache.parquet.filter2.predicate.FilterApi.*;

public class Example1 {
    public static void main(String args[]) throws IOException {

        Job job = Job.getInstance();

//        ParquetInputFormat.setReadSupportClass(job, ProtoReadSupport.class);
//        ParquetOutputFormat.setWriteSupportClass(job, ProtoWriteSupport.class);
//        ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
//        ProtoParquetOutputFormat.setProtobufClass(job,SpatioTemporal.class);

        ProtoParquetInputFormat.setReadSupportClass(job, ProtoReadSupport.class);
        ProtoParquetInputFormat.setFilterPredicate(job.getConfiguration(), /*FilterApi.gtEq(FilterApi.userDefined(doubleColumn(""),new CustomFilter(4d)))*/and(gtEq(doubleColumn("part.coordinate.x"), 26d),gtEq(doubleColumn("part.coordinate.y"), 26d)));
        //ProtoParquetInputFormat.setRequestedProjection(job, "message SpatioTemporal { required int64 timestamp; }");
        ProtoParquetOutputFormat.setWriteSupportClass(job, ProtoWriteSupport.class);
        //ProtoParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        ProtoParquetOutputFormat.setProtobufClass(job,Geometry.class);
        ProtoParquetOutputFormat.setBlockSize(job, 5242880);
        ContextUtil.getConfiguration(job).setBoolean(ProtoParquetInputFormat.DICTIONARY_FILTERING_ENABLED, false);
        ContextUtil.getConfiguration(job).setBoolean(ProtoParquetInputFormat.BLOOM_FILTERING_ENABLED, false);
        ContextUtil.getConfiguration(job).setBoolean(ProtoParquetInputFormat.COLUMN_INDEX_FILTERING_ENABLED, false);
        ContextUtil.getConfiguration(job).setBoolean(ProtoParquetInputFormat.STATS_FILTERING_ENABLED, false);


        SimpleDateFormat sdf =  new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        SparkConf sparkConf = new SparkConf().setMaster("local[1]").set("spark.executor.memory","1g");;//.set("spark.kryo.registrationRequired", "true").registerKryoClasses(new Class[]{Grid.class, Agreement.class, Agreements.class, Edge.class, Edge[].class,  Space.class, Cell.class, NewFunc.class, Position.class, TypeSet.class, java.util.HashMap.class, Point.class, Rectangle.class, ArrayList.class, java.lang.invoke.SerializedLambda.class, org.apache.spark.util.collection.CompactBuffer[].class, org.apache.spark.util.collection.CompactBuffer.class, scala.reflect.ManifestFactory$.class,scala.reflect.ManifestFactory$.MODULE$.Any().getClass(), ConcurrentHashMap.class,  UnsafeHashedRelation.class,org.apache.spark.sql.execution.columnar.DefaultCachedBatch.class, byte[][].class, org.apache.spark.sql.catalyst.expressions.GenericInternalRow.class, org.apache.spark.unsafe.types.UTF8String.class});
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        Random random = new Random();

//        List<Pair<Double,Double>[]> pairs = new ArrayList<>();
//        pairs.add(new Pair[]{new Pair(50d,50d), new Pair(100d,100d), new Pair(150d,150d)});
//        pairs.add(new Pair[]{new Pair(350d,350d), new Pair(450d,450d), new Pair(1000d,1000d)});
//        pairs.add(new Pair[]{new Pair(350d,350d), new Pair(450d,1000d), new Pair(1000d,1000d)});
//        pairs.add(new Pair[]{new Pair(350d,350d), new Pair(450d,450d), new Pair(1000d,1000d)});
//        pairs.add(new Pair[]{new Pair(120d,120d)});
//        pairs.add(new Pair[]{new Pair(30d,30d)});
//
//        JavaPairRDD<Void, Geometry> rdd = jsc.parallelize(pairs).map(pairArray->{
//            Geometry.Part.Builder part = Geometry.Part.newBuilder();
//            for (Pair<Double, Double> doubleDoublePair : pairArray) {
//                part.addCoordinate(Geometry.Part.Coordinate.newBuilder().setX(doubleDoublePair.getKey().doubleValue()).setY(doubleDoublePair.getValue().doubleValue()).build());
//            }
//            return Geometry.newBuilder().setGeometry(2).addPart(part.build()).build();
//        }).flatMapToPair(geometry->{
//            List<Tuple2<Void, Geometry>> l = new ArrayList<>();
//            l.add(new Tuple2<>(null, geometry));
//            return l.iterator();
//        });
//
//        rdd.saveAsNewAPIHadoopFile("src/main/resources/example", Void.class, Geometry.class, ParquetOutputFormat.class, job.getConfiguration());

        JavaPairRDD<Void, Geometry.Builder> rdd2 = jsc.newAPIHadoopFile("src/main/resources/example",ProtoParquetInputFormat.class, Void.class, Geometry.Builder.class,job.getConfiguration());

//        long k = System.currentTimeMillis();
//        JavaRDD<SpatioTemporal> rd = rdd2.filter(t->t._2.getId()==1421).map(t-> t._2.build());
//        System.out.println("Counted: "+rd.collect().size());
//        //System.out.println("Counted: "+rdd2.count());
//        System.out.println((System.currentTimeMillis()-k)/1000+" sec");
        for (int i = 0; i < 3; i++) {
            long k = System.currentTimeMillis();
            System.out.println("Counted: "+rdd2.count());
            //System.out.println("Counted: "+j);
            //System.out.println("Counted: "+rdd2.count());
            System.out.println((System.currentTimeMillis()-k)/1000+" sec");
        }


    }
}
