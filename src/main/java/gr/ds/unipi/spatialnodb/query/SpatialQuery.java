package gr.ds.unipi.spatialnodb.query;

import com.uber.h3core.H3Core;
import gr.ds.unipi.grid.Grid;
import gr.ds.unipi.grid.Pair;
import gr.ds.unipi.shapes.Rectangle;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.columnar.DOUBLE;

import java.util.List;

public class SpatialQuery {

    private final String path;
    private final Rectangle rectangleQuery;

    private final int longitudeIndex;
    private final int latitudeIndex;
    private final String delimiter;

    private final String longitudeColumnName;
    private final String latitudeColumnName;

    private final H3Core h3Core;
    private final Grid grid;


    public SpatialQuery(String path,Rectangle rectangleQuery, int longitudeIndex, int latitudeIndex, String delimiter){
        this.path = path;
        this.rectangleQuery = rectangleQuery;
        this.longitudeIndex = longitudeIndex;
        this.latitudeIndex = latitudeIndex;
        this.delimiter = delimiter;

        this.h3Core = null;
        this.grid = null;
        this.longitudeColumnName=null;
        this.latitudeColumnName=null;
    }

    public SpatialQuery(String path, Rectangle rectangleQuery, String longitudeColumnName, String latitudeColumnName, H3Core h3Core){
        this.path = path;
        this.rectangleQuery = rectangleQuery;
        this.longitudeColumnName=longitudeColumnName;
        this.latitudeColumnName=latitudeColumnName;
        this.h3Core = h3Core;

        this.grid = null;
        this.longitudeIndex = Integer.MIN_VALUE;
        this.latitudeIndex = Integer.MIN_VALUE;
        this.delimiter = null;
    }

    public SpatialQuery(String path, Rectangle rectangleQuery, String longitudeColumnName, String latitudeColumnName, Grid grid){
        this.path = path;
        this.rectangleQuery = rectangleQuery;
        this.longitudeColumnName=longitudeColumnName;
        this.latitudeColumnName=latitudeColumnName;
        this.grid = grid;

        this.h3Core = null;
        this.longitudeIndex = Integer.MIN_VALUE;
        this.latitudeIndex = Integer.MIN_VALUE;
        this.delimiter = null;
    }

    public void startQuerying(){
        SparkConf sparkConf = new SparkConf().set("spark.serializer","org.apache.spark.serializer.KryoSerializer");//.set("spark.kryo.registrationRequired", "true").registerKryoClasses(new Class[]{Grid.class, Agreement.class, Agreements.class, Edge.class, Edge[].class,  Space.class, Cell.class, NewFunc.class, Position.class, TypeSet.class, java.util.HashMap.class, Point.class, Rectangle.class, ArrayList.class, java.lang.invoke.SerializedLambda.class, org.apache.spark.util.collection.CompactBuffer[].class, org.apache.spark.util.collection.CompactBuffer.class, scala.reflect.ManifestFactory$.class,scala.reflect.ManifestFactory$.MODULE$.Any().getClass(), ConcurrentHashMap.class,  UnsafeHashedRelation.class,org.apache.spark.sql.execution.columnar.DefaultCachedBatch.class, byte[][].class, org.apache.spark.sql.catalyst.expressions.GenericInternalRow.class, org.apache.spark.unsafe.types.UTF8String.class});
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        long startTime = System.currentTimeMillis();

        Dataset<Row> df = null;

        if(grid != null || h3Core !=null){
            df = sparkSession.read().parquet(path);

            Column longitudeCol = new Column(longitudeColumnName);
            Column latitudeCol = new Column(latitudeColumnName);
            Column index = new Column("index");

            if(grid != null){
                System.out.println("Grid is used for querying");
                Column condition;
//                Column secondCondition;
                List<Pair<Long, Long>> pairs = grid.getCellsIds(rectangleQuery);
                condition = index.$greater$eq(pairs.get(0).getKey()).and(index.$less$eq(pairs.get(0).getValue()));

//                if(pairs.size()>1 && !(pairs.get(0).getKey().equals(pairs.get(0).getValue()))){
//                    System.out.println("Big Condition");
//                    for (int i = 1; i < pairs.size()-1; i++) {
//                        condition = condition.or(index.$eq$eq$eq(pairs.get(i).getKey()).or(index.$eq$eq$eq(pairs.get(i).getValue())));
//                    }
//                    condition = condition.or(index.$greater$eq(pairs.get(pairs.size()-1).getKey()).and(index.$less$eq(pairs.get(pairs.size()-1).getValue())));
//                    condition = condition.and(longitudeCol.$greater$eq(rectangleQuery.getLowerBound().getX()).and(longitudeCol.$less$eq(rectangleQuery.getUpperBound().getX())));
//                    condition = condition.and(latitudeCol.$greater$eq(rectangleQuery.getLowerBound().getY()).and(latitudeCol.$less$eq(rectangleQuery.getUpperBound().getY())));
//
//                    secondCondition = index.$greater$eq(pairs.get(1).getKey()+1).and(index.$less$eq(pairs.get(1).getValue()-1));
//                    for (int i = 2; i < pairs.size()-1; i++) {
//                        secondCondition = secondCondition.or(index.$greater$eq(pairs.get(i).getKey()+1).and(index.$less$eq(pairs.get(i).getValue()-1)));
//                    }
//                    df = df.filter(condition.or(secondCondition));
//                }else{
                    for (int i = 1; i < pairs.size(); i++) {
                        condition = condition.or(index.$greater$eq(pairs.get(i).getKey()).and(index.$less$eq(pairs.get(i).getValue())));
                    }
                    df= df.filter(condition);
                    df = df.filter(longitudeCol.$greater$eq(rectangleQuery.getLowerBound().getX()).and(longitudeCol.$less$eq(rectangleQuery.getUpperBound().getX())));
                    df = df.filter(latitudeCol.$greater$eq(rectangleQuery.getLowerBound().getY()).and(latitudeCol.$less$eq(rectangleQuery.getUpperBound().getY())));
//                }
            }else {
                //h3 index
            }
        }else{
            df = sparkSession.read().option("delimiter", delimiter).csv(path).withColumn("_c"+longitudeIndex, new Column("_c"+longitudeIndex).cast(DOUBLE.toString())).withColumn("_c"+latitudeIndex, new Column("_c"+latitudeIndex).cast(DOUBLE.toString()));

            Column longitudeCol = new Column("_c"+longitudeIndex);
            Column latitudeCol = new Column("_c"+latitudeIndex);

            df = df.filter(longitudeCol.$greater$eq(rectangleQuery.getLowerBound().getX()).and(longitudeCol.$less$eq(rectangleQuery.getUpperBound().getX())));
            df = df.filter(latitudeCol.$greater$eq(rectangleQuery.getLowerBound().getY()).and(latitudeCol.$less$eq(rectangleQuery.getUpperBound().getY())));
        }

        System.out.println("Count results: "+df.count());
        System.out.println("Elapsed Time (Sec): " + ((System.currentTimeMillis()-startTime)/1000));
    }

}
