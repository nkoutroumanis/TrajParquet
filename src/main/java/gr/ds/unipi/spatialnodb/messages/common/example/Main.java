package gr.ds.unipi.spatialnodb.messages.common.example;

import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Random;

import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;

public class Main {
    public static void main(String args[]) throws IOException {

        Job job = Job.getInstance();

//        ParquetInputFormat.setReadSupportClass(job, ProtoReadSupport.class);
//        ParquetOutputFormat.setWriteSupportClass(job, ProtoWriteSupport.class);
//        ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
//        ProtoParquetOutputFormat.setProtobufClass(job,SpatioTemporal.class);

        String message = "message Line {\n" +
                "required DOUBLE minX;\n" +
                "required DOUBLE maxX;\n" +
                "required DOUBLE minY;\n" +
                "required DOUBLE maxY;\n" +
                "}";

//        System.out.println(ParquetWriteSupport.SPARK_ROW_SCHEMA());
        ParquetInputFormat.setReadSupportClass(job, LineReadSupport.class);
        ParquetInputFormat.setFilterPredicate(job.getConfiguration(), /*FilterApi.gtEq(FilterApi.userDefined(doubleColumn(""),new CustomFilter(4d)))*/gtEq(doubleColumn("minX"), -7d));
        //ProtoParquetInputFormat.setRequestedProjection(job, "message SpatioTemporal { required int64 timestamp; }");
        ParquetOutputFormat.setWriteSupportClass(job, LineWriteSupport.class);
        //ProtoParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
//        ParquetOutputFormat.setProtobufClass(job, Geometry.class);
        ParquetOutputFormat.setBlockSize(job, 5242880);

        SimpleDateFormat sdf =  new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        SparkConf sparkConf = new SparkConf().setMaster("local[1]").set("spark.executor.memory","1g");;//.set("spark.kryo.registrationRequired", "true").registerKryoClasses(new Class[]{Grid.class, Agreement.class, Agreements.class, Edge.class, Edge[].class,  Space.class, Cell.class, NewFunc.class, Position.class, TypeSet.class, java.util.HashMap.class, Point.class, Rectangle.class, ArrayList.class, java.lang.invoke.SerializedLambda.class, org.apache.spark.util.collection.CompactBuffer[].class, org.apache.spark.util.collection.CompactBuffer.class, scala.reflect.ManifestFactory$.class,scala.reflect.ManifestFactory$.MODULE$.Any().getClass(), ConcurrentHashMap.class,  UnsafeHashedRelation.class,org.apache.spark.sql.execution.columnar.DefaultCachedBatch.class, byte[][].class, org.apache.spark.sql.catalyst.expressions.GenericInternalRow.class, org.apache.spark.unsafe.types.UTF8String.class});
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        Random random = new Random();

//        List<Line> pairs = new ArrayList<>();
//        pairs.add(new Line(0,0,100,100,new double[]{100d, 34d, 23121d, 12312d}));
//
//        JavaPairRDD<Void, Line> rdd = jsc.parallelize(pairs).flatMapToPair(line->{
//            List<Tuple2<Void, Line>> l = new ArrayList<>();
//            l.add(new Tuple2<>(null, line));
//            return l.iterator();
//        });
////
//        rdd.saveAsNewAPIHadoopFile("src/main/resources/example", Void.class, Line.class, ParquetOutputFormat.class, job.getConfiguration());

        JavaPairRDD<Void, Line> rdd2 = (JavaPairRDD<Void, Line>) jsc.newAPIHadoopFile("src/main/resources/example",ParquetInputFormat.class, Void.class, Line.class,job.getConfiguration());

        for (int i = 0; i < 3; i++) {
            long k = System.currentTimeMillis();
            System.out.println("Counted: "+rdd2.count());
            System.out.println((System.currentTimeMillis()-k)/1000+" sec");
        }


    }
}
