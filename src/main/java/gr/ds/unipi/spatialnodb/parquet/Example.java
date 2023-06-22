package gr.ds.unipi.spatialnodb.parquet;

import gr.ds.unipi.spatialnodb.messages.proto.trajparquet.SpatioTemporal;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Random;

import static org.apache.parquet.filter2.predicate.FilterApi.*;

public class Example {
    public static void main(String args[]) throws IOException {

        Job job = Job.getInstance();

//        ParquetInputFormat.setReadSupportClass(job, ProtoReadSupport.class);
//        ParquetOutputFormat.setWriteSupportClass(job, ProtoWriteSupport.class);
//        ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
//        ProtoParquetOutputFormat.setProtobufClass(job,SpatioTemporal.class);

        ParquetOutputFormat.setWriteSupportClass(job, ParquetWriteSupport.class);
        ProtoParquetInputFormat.setReadSupportClass(job, ProtoReadSupport.class);
        //ProtoParquetInputFormat.setUnboundRecordFilter(job,Fil.class);
        ProtoParquetInputFormat.setFilterPredicate(job.getConfiguration(), gt(doubleColumn("coordinates.longitude"), 26d));
        //ProtoParquetInputFormat.setRequestedProjection(job, "message SpatioTemporal { required int64 timestamp; }");
        ProtoParquetOutputFormat.setWriteSupportClass(job, ProtoWriteSupport.class);
        ProtoParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        ProtoParquetOutputFormat.setProtobufClass(job,SpatioTemporal.class);
        ProtoParquetOutputFormat.setBlockSize(job, 5242880);


//        new ParquetFilters(SpatioTemporal,true, true,true,true,1,true, null);
//        ParquetInputFormat.setFilterPredicate(job, new FilterPredicate() {
//            @Override
//            public <SpatioTemporal> SpatioTemporal accept(Visitor<SpatioTemporal> visitor) {
//                return visitor.visit();
//            }
//        });

        SimpleDateFormat sdf =  new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        SparkConf sparkConf = new SparkConf().setMaster("local[1]").set("spark.executor.memory","1g");;//.set("spark.kryo.registrationRequired", "true").registerKryoClasses(new Class[]{Grid.class, Agreement.class, Agreements.class, Edge.class, Edge[].class,  Space.class, Cell.class, NewFunc.class, Position.class, TypeSet.class, java.util.HashMap.class, Point.class, Rectangle.class, ArrayList.class, java.lang.invoke.SerializedLambda.class, org.apache.spark.util.collection.CompactBuffer[].class, org.apache.spark.util.collection.CompactBuffer.class, scala.reflect.ManifestFactory$.class,scala.reflect.ManifestFactory$.MODULE$.Any().getClass(), ConcurrentHashMap.class,  UnsafeHashedRelation.class,org.apache.spark.sql.execution.columnar.DefaultCachedBatch.class, byte[][].class, org.apache.spark.sql.catalyst.expressions.GenericInternalRow.class, org.apache.spark.unsafe.types.UTF8String.class});
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        Random random = new Random();

//        JavaPairRDD<Void, SpatioTemporal> rdd = jsc.textFile("src/main/resources/data*").map(t->{
//            String[] elements = t.split(";");
//            return SpatioTemporal.newBuilder().setId(/*Integer.parseInt(elements[0].replace("_","").substring(0,4))*/random.nextInt(1000)).setCoordinates(SpatioTemporal.Coordinates.newBuilder().setLongitude(Double.parseDouble(elements[1])).setLatitude(Double.parseDouble(elements[2])).build()).setTimestamp(sdf.parse(elements[3]).getTime()).build();
//        }).sortBy(f->f.getTimestamp(),true,1).flatMapToPair(t->{
//            List<Tuple2<Void, SpatioTemporal>> l = new ArrayList<>();
//            l.add(new Tuple2<>(null, t));
//            return l.iterator();
//        });
//
//        rdd.saveAsNewAPIHadoopFile("src/main/resources/df-temporal", Void.class, SpatioTemporal.class, ParquetOutputFormat.class, job.getConfiguration());

        JavaPairRDD<Void, SpatioTemporal.Builder> rdd2 = jsc.newAPIHadoopFile("src/main/resources/df-id",ParquetInputFormat.class, Void.class, SpatioTemporal.Builder.class,job.getConfiguration());

//        long k = System.currentTimeMillis();
//        JavaRDD<SpatioTemporal> rd = rdd2.filter(t->t._2.getId()==1421).map(t-> t._2.build());
//        System.out.println("Counted: "+rd.collect().size());
//        //System.out.println("Counted: "+rdd2.count());
//        System.out.println((System.currentTimeMillis()-k)/1000+" sec");
        for (int i = 0; i < 3; i++) {
            long k = System.currentTimeMillis();
            long j = rdd2.count();
            System.out.println("Counted: "+j);
            //System.out.println("Counted: "+rdd2.count());
            System.out.println((System.currentTimeMillis()-k)/1000+" sec");
        }


    }
}
