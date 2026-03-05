package gr.ds.unipi.spatialnodb.queries.trajparquet;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.io.api.Binary;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.Ranges;
import org.davidmoten.hilbert.SmallHilbertCurve;

import java.io.*;
import java.util.*;

import static gr.ds.unipi.spatialnodb.AppConfig.loadConfig;
import static gr.ds.unipi.spatialnodb.dataloading.HilbertUtil.doesTrajectoryIntersectWithCube;
import static org.apache.parquet.filter2.predicate.FilterApi.*;

public class KnnQueriesDirectoriesBruteForce {
    public static void main(String args[]) throws IOException {

        Config config = loadConfig("knnQuery.conf");

        Config dataLoading = config.getConfig("queries");
        final String parquetPath = dataLoading.getString("parquetPath");
        final String queriesFilePath = dataLoading.getString("queriesFilePath");
        final String queriesFileExport = dataLoading.getString("queriesFileExport");
        final int k = dataLoading.getInt("k");

        Config metadata = ConfigFactory.parseFile(new File(parquetPath+ File.separator+"space.metadata")).resolve().getConfig("grid3DHilbert");
        final int bits = metadata.getInt("bits");
        Config boundaries = metadata.getConfig("boundaries");
        final double minLon = boundaries.getDouble("minLon");
        final double minLat = boundaries.getDouble("minLat");
        final long minTime = boundaries.getLong("minTime");
        final double maxLon = boundaries.getDouble("maxLon");
        final double maxLat = boundaries.getDouble("maxLat");
        final long maxTime = boundaries.getLong("maxTime");

        final SmallHilbertCurve hilbertCurve = HilbertCurve.small().bits(bits).dimensions(3);
        final long maxOrdinates = hilbertCurve.maxOrdinate();

        Job job = Job.getInstance();
//        job.getConfiguration().set("partialTrajectory", "true");

        ParquetInputFormat.setReadSupportClass(job, TrajectorySegmentReadSupport.class);

        SparkConf sparkConf = new SparkConf().registerKryoClasses(new Class[]{SpatioTemporalPoint.class,SpatioTemporalPoint[].class}).setMaster("local[*]").set("spark.executor.memory","1g").set("spark.kryoserializer.buffer.max","2047m");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        File[] directories = new File(parquetPath+File.separator+"stIndex").listFiles(File::isDirectory);
        Set<String> directoriesSet = new HashSet<>();
        for (File directory : directories) {
            directoriesSet.add(directory.getName());
        }

        List<Long> times = new ArrayList<>();

        BufferedWriter bw = new BufferedWriter(new FileWriter(queriesFileExport));
        BufferedReader br = new BufferedReader(new FileReader(queriesFilePath));
        String query;
        while ((query = br.readLine()) != null) {

            int pointsCount = countPoints(query);
            SpatioTemporalPoint[] trajectoryQuery = new SpatioTemporalPoint[pointsCount];
            char[] chars = query.toCharArray();
            int ichar = 0;
            int idx = 0;
            int len = chars.length;

            double mbrMinLongitude = Double.MAX_VALUE;
            double mbrMinLatitude= Double.MAX_VALUE;
            long mbrMinTimestamp= Long.MAX_VALUE;

            double mbrMaxLongitude= -Double.MAX_VALUE;
            double mbrMaxLatitude= -Double.MAX_VALUE;
            long mbrMaxTimestamp= Long.MIN_VALUE;

            while (ichar < len) {
                // Parse longitude
                int start = ichar;
                while (chars[ichar] != ',') ichar++;
                double lon = parseDouble(chars, start, ichar);
                ichar++; // skip ','

                // Parse latitude
                start = ichar;
                while (chars[ichar] != ',') ichar++;
                double lat = parseDouble(chars, start, ichar);
                ichar++; // skip ','

                // Parse timestamp
                start = ichar;
                while (chars[ichar] != ';') ichar++;
                long tstp = parseLong(chars, start, ichar);
                ichar++; // skip ';'

                trajectoryQuery[idx++] = new SpatioTemporalPoint(lon,lat,tstp);

                if(Double.compare(lon,mbrMaxLongitude)==1){
                    mbrMaxLongitude = lon;
                }
                if(Double.compare(lat,mbrMaxLatitude)==1){
                    mbrMaxLatitude = lat;
                }
                if(Double.compare(tstp,mbrMaxTimestamp)==1){
                    mbrMaxTimestamp = tstp;
                }

                if(Double.compare(lon,mbrMinLongitude)==-1){
                    mbrMinLongitude = lon;
                }
                if(Double.compare(lat,mbrMinLatitude)==-1){
                    mbrMinLatitude = lat;
                }
                if(Double.compare(tstp,mbrMinTimestamp)==-1){
                    mbrMinTimestamp = tstp;
                }
            }

//            System.out.println(jsc.newAPIHadoopFile(parquetPath + "/" + "idIndex", ParquetInputFormat.class, Void.class, TrajectorySegment.class, job.getConfiguration()).count());
//            System.exit(0);

            JavaPairRDD<Void, TrajectorySegment> pairRDD = (JavaPairRDD<Void, TrajectorySegment>) jsc.newAPIHadoopFile(parquetPath + "/" + "idIndex", ParquetInputFormat.class, Void.class, TrajectorySegment.class, job.getConfiguration());
            List<TrajectoryScore> ts = pairRDD.map(t -> TrajectoryScore.newTrajectoryScore(t._2, HilbertUtil.frechetDistance(trajectoryQuery, t._2.getSpatioTemporalPoints()))).collect();
            BoundedPriorityQueue trajectoryQueue = BoundedPriorityQueue.newBoundedPriorityQueue(k);
            ts.forEach(trajectoryQueue::add);


            trajectoryQueue.getMaxHeap().forEach(d->{
                System.out.println("object id:"+d.getTrajectorySegment().getObjectId()+" score:"+d.getScore());
            });

            System.out.println(trajectoryQueue.getMaxScore());
        }
    }

    private static void flush(HashMap<String, List<TrajectorySegment>> identifiedTrajectories, HashSet<String> flushedtrajectories){
        identifiedTrajectories.entrySet().removeIf(e->{
            if(e.getValue().get(0).getSegment()==-1){
                flushedtrajectories.add(e.getValue().get(0).getObjectId());
                return true;
            }else if(e.getValue().get(0).getSegment()==1 && e.getValue().get(e.getValue().size()-1).getSegment()<1){
                for (int i = 0; i < e.getValue().size()-1; i++) {
                    if(e.getValue().get(i).getSegment()+1!=Math.abs(e.getValue().get(i+1).getSegment())){
                        return false;
                    }
                }
                flushedtrajectories.add(e.getValue().get(0).getObjectId());
                return true;
            }
            return false;
        });
    }

    private static int countPoints(String line) {
        int count = 0;
        for (int i = 0; i < line.length(); i++) {
            if (line.charAt(i) == ';') count++;
        }
        return count;
    }

    private static double parseDouble(char[] chars, int start, int end) {
        return Double.parseDouble(new String(chars, start, end - start));
    }

    // Parse a long directly from char[] between start (inclusive) and end (exclusive)
    private static long parseLong(char[] chars, int start, int end) {
        long result = 0;
        boolean neg = false;
        int i = start;
        if (chars[i] == '-') { neg = true; i++; }

        while (i < end && chars[i] >= '0' && chars[i] <= '9') {
            result = result * 10 + (chars[i] - '0');
            i++;
        }

        return neg ? -result : result;
    }

}
