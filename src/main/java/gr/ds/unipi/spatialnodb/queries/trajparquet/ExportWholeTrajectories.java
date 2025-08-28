package gr.ds.unipi.spatialnodb.queries.trajparquet;

import com.typesafe.config.Config;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.davidmoten.hilbert.SmallHilbertCurve;
import scala.Tuple3;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static gr.ds.unipi.spatialnodb.AppConfig.loadConfig;

public class ExportWholeTrajectories {
    public static void main(String[] args) {
        Config config = loadConfig("data-loading.conf");

        Config dataLoading = config.getConfig("data-loading");
        final String rawDataPath = dataLoading.getString("rawDataPath");
        final String writePath = dataLoading.getString("writePath");
        final int objectIdIndex = dataLoading.getInt("objectIdIndex");
        final int longitudeIndex = dataLoading.getInt("longitudeIndex");
        final int latitudeIndex = dataLoading.getInt("latitudeIndex");
        final int timeIndex = dataLoading.getInt("timeIndex");
        final String dateFormat = dataLoading.getString("dateFormat");
        final String delimiter = dataLoading.getString("delimiter");


        SimpleDateFormat sdf =  new SimpleDateFormat(dateFormat);
        SparkConf sparkConf = new SparkConf()/*.setMaster("local[1]").set("spark.executor.memory","1g")*/.registerKryoClasses(new Class[]{SmallHilbertCurve.class, HilbertUtil.class});
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        List<List<Tuple3<Double, Double, Long>>> trajectories = jsc.textFile(rawDataPath).map(f->f.split(delimiter)).groupBy(f-> f[objectIdIndex]).map(f->{
            List<Tuple3<Double, Double, Long>> tuple = new ArrayList<>();
            for (String[] strings : f._2) {
                long timestamp = -1;
                try {
                    timestamp = sdf.parse(strings[timeIndex]).getTime();
                }catch (Exception e){
                    continue;
                }
                tuple.add(Tuple3.apply(Double.parseDouble(strings[longitudeIndex]), Double.parseDouble(strings[latitudeIndex]), timestamp));
            }

            Comparator<Tuple3<Double, Double, Long>> comp = Comparator.comparingLong(d-> d._3());
            comp = comp.thenComparingDouble(d-> d._1());
            comp = comp.thenComparingDouble(d-> d._2());
            tuple.sort(comp);

            return tuple;
        }).takeSample(false, 1000);


        try (BufferedWriter writer = new BufferedWriter(new FileWriter(writePath))) {
            for (List<Tuple3<Double, Double, Long>> trajectory : trajectories) {
                for (Tuple3<Double, Double, Long> doubleDoubleLongTuple3 : trajectory) {
                    writer.write(doubleDoubleLongTuple3._1()+","+doubleDoubleLongTuple3._2()+","+doubleDoubleLongTuple3._3()+";");
                }
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
