package gr.ds.unipi.spatialnodb.dataloading.uber;

import com.uber.h3core.H3Core;
import com.uber.h3core.H3CoreLoader;
import gr.ds.unipi.TypeSet;
import gr.ds.unipi.agreements.Agreement;
import gr.ds.unipi.agreements.Agreements;
import gr.ds.unipi.agreements.Edge;
import gr.ds.unipi.agreements.Space;
import gr.ds.unipi.grid.Cell;
import gr.ds.unipi.grid.Grid;
import gr.ds.unipi.grid.NewFunc;
import gr.ds.unipi.grid.Position;
import gr.ds.unipi.shapes.Point;
import gr.ds.unipi.shapes.Rectangle;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.columnar.DOUBLE;
import org.apache.spark.sql.execution.joins.UnsafeHashedRelation;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import scala.reflect.ClassTag;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

public class DataLoading {

    private final String rawDataPath;
    private final String writePath;
    private final int longitudeIndex;
    private final int latitudeIndex;
    private final int idIndex;
    private final String delimiter;

    private final int resolution;

    private final double radius;
    private final double minLon;
    private final double minLat;
    private final double maxLon;
    private final double maxLat;


    private final String index;

    public DataLoading(String rawDataPath, String writePath, int longitudeIndex, int latitudeIndex, int idIndex, String delimiter, int resolution) {
        this.rawDataPath = rawDataPath;
        this.writePath = writePath;
        this.longitudeIndex = longitudeIndex;
        this.latitudeIndex = latitudeIndex;
        this.idIndex = idIndex;
        this.delimiter = delimiter;

        this.resolution = resolution;

        this.radius = -1;
        this.minLon = Integer.MAX_VALUE;
        this.minLat = Integer.MAX_VALUE;
        this.maxLon = Integer.MIN_VALUE;
        this.maxLat = Integer.MIN_VALUE;;

        this.index = "U3";
        System.out.println("U3 indexing is used");
    }

    public DataLoading(String rawDataPath, String writePath, int longitudeIndex, int latitudeIndex, int idIndex, String delimiter, double radius, double minLon, double minLat, double maxLon, double maxLat) {
        this.rawDataPath = rawDataPath;
        this.writePath = writePath;
        this.longitudeIndex = longitudeIndex;
        this.latitudeIndex = latitudeIndex;
        this.idIndex = idIndex;
        this.delimiter = delimiter;

        this.resolution = -1;

        this.radius = radius;
        this.minLon = minLon;
        this.minLat = minLat;
        this.maxLon = maxLon;
        this.maxLat = maxLat;
        this.index = "Grid";
        System.out.println("Grid indexing is used");
    }

    public void startLoading() throws IOException {
        H3Core h3 = H3Core.newInstance();
        long hexLong = h3.latLngToCell(39.153, -88.578, 9);
        System.out.println(hexLong);
        //System.load("execstack -c /home/user/NoDB/lib/h3-4.0.2.jar");
        SparkConf sparkConf = new SparkConf().set("spark.serializer","org.apache.spark.serializer.KryoSerializer");//.set("spark.kryo.registrationRequired", "true").registerKryoClasses(new Class[]{Grid.class, Agreement.class, Agreements.class, Edge.class, Edge[].class,  Space.class, Cell.class, NewFunc.class, Position.class, TypeSet.class, java.util.HashMap.class, Point.class, Rectangle.class, ArrayList.class, java.lang.invoke.SerializedLambda.class, org.apache.spark.util.collection.CompactBuffer[].class, org.apache.spark.util.collection.CompactBuffer.class, scala.reflect.ManifestFactory$.class,scala.reflect.ManifestFactory$.MODULE$.Any().getClass(), ConcurrentHashMap.class,  UnsafeHashedRelation.class,org.apache.spark.sql.execution.columnar.DefaultCachedBatch.class, byte[][].class, org.apache.spark.sql.catalyst.expressions.GenericInternalRow.class, org.apache.spark.unsafe.types.UTF8String.class});
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        long startTime = System.currentTimeMillis();

        Dataset<Row> df = sparkSession.read().option("delimiter", delimiter).csv(rawDataPath).withColumnRenamed("_c"+idIndex, "id").withColumnRenamed("_c"+longitudeIndex, "longitude").withColumnRenamed("_c"+latitudeIndex, "latitude");

        UserDefinedFunction index = null;

        if(this.index.equals("U3")){
//            H3Core h3 = H3Core.newInstance();
            Broadcast<H3Core> indexBr = jsc.broadcast(h3);

            //Broadcast<H3Core> indexBr = sparkSession.sparkContext().broadcast(h3, ClassTag.apply(H3Core.class));
            index = udf(
                    (String x, String y) -> indexBr.getValue().latLngToCell(Double.parseDouble(y),Double.parseDouble(x),9), DataTypes.LongType
            );
        }else if(this.index.equals("Grid")){
            Grid grid = Grid.newGeoGrid(Rectangle.newRectangle(Point.newPoint(minLon, minLat), Point.newPoint(maxLon, maxLat)), radius, NewFunc.datasetB);
            Broadcast<Grid> indexBr = jsc.broadcast(grid);

            //Broadcast<Grid> indexBr = sparkSession.sparkContext().broadcast(grid, ClassTag.apply(Grid.class));
            index = udf(
                    (String x, String y) -> Long.parseLong(indexBr.getValue().getPartitionsAType(Double.parseDouble(x),Double.parseDouble(y))[0]), DataTypes.LongType
            );

        }else{
            try {
                throw new Exception("Not correct index has been set");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        df = df.withColumn("index", index.apply(col("longitude"),col("latitude"))).withColumn("longitude",col("longitude").cast(DOUBLE.toString())).withColumn("latitude",col("latitude").cast(DOUBLE.toString())).repartition(col("index"));

        df.write().partitionBy("index").parquet(writePath);
        System.out.println("Elapsed Time (Sec): " + ((System.currentTimeMillis()-startTime)/1000));
        sparkSession.close();

    }
}
