package gr.ds.unipi.spatialnodb.queries.trajparquet;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
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

import static gr.ds.unipi.spatialnodb.AppConfig.loadConfig;

public class QueriesDirectoriesFrechetIndex {
    public static void main(String args[]) throws IOException {

        Config config = loadConfig("queries-frechet.conf");

        Config dataLoading = config.getConfig("queries");
        final String parquetPath = dataLoading.getString("parquetPath");
        final String queriesFilePath = dataLoading.getString("queriesFilePath");
        final String queriesFileExport = dataLoading.getString("queriesFileExport");
        final double epsilon = dataLoading.getDouble("epsilon");

        Config metadata = ConfigFactory.parseFile(new File(parquetPath+ File.separator+"metadata.json")).resolve();
        final int bits = metadata.getInt("3d-grid-hilbert.bits");
        final double minLon = metadata.getDouble("3d-grid-hilbert.boundaries.minLon");
        final double minLat = metadata.getDouble("3d-grid-hilbert.boundaries.minLat");
        final long minTime = metadata.getLong("3d-grid-hilbert.boundaries.minTime");
        final double maxLon = metadata.getDouble("3d-grid-hilbert.boundaries.maxLon");
        final double maxLat = metadata.getDouble("3d-grid-hilbert.boundaries.maxLat");
        final long maxTime = metadata.getLong("3d-grid-hilbert.boundaries.maxTime");

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

            int pointsCount = countPoints(query);
            SpatioTemporalPoint[] trajectoryQuery = new SpatioTemporalPoint[pointsCount];
            char[] chars = query.toCharArray();
            int ichar = 0;
            int idx = 0;
            int len = chars.length;

            double mbrMinLongitude = Double.MAX_VALUE;
            double mbrMinLatitude= Double.MAX_VALUE;
            long mbrMinTimestamp= Long.MAX_VALUE;

            double mbrMaxLongitude= -Double.MIN_VALUE;
            double mbrMaxLatitude= -Double.MIN_VALUE;
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

            final double queryMinLongitude = Double.max(minLon,mbrMinLongitude-epsilon);
            final double queryMinLatitude = Double.max(minLat,mbrMinLatitude-epsilon);
            final long queryMinTimestamp = minTime;

            final double queryMaxLongitude = Double.min(maxLon-0.0000001,mbrMaxLongitude+epsilon);
            final double queryMaxLatitude = Double.min(maxLat-0.0000001,mbrMaxLatitude+epsilon);
            final long queryMaxTimestamp = maxTime-1000;

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
                    .filter(f -> {
                        SpatioTemporalPoint[] spatioTemporalPoints = f._2.getSpatioTemporalPoints();
                        for (int i = 0; i < spatioTemporalPoints.length; i++) {
                            if(!HilbertUtil.inBox(spatioTemporalPoints[i].getLongitude(), spatioTemporalPoints[i].getLatitude(),spatioTemporalPoints[i].getTimestamp(),queryMinLongitude, queryMinLatitude, queryMinTimestamp, queryMaxLongitude, queryMaxLatitude, queryMaxTimestamp)) {
                                return false;
                            }
                        }

                        if((f._2.getSpatioTemporalPoints().length>2 || !(f._2.getSegment()>1)) && HilbertUtil.isMinDistGreaterThan(f._2.getMinLongitude(), f._2.getMinLatitude(), f._2.getMaxLongitude(), f._2.getMaxLatitude(), trajectoryQuery, epsilon)){
                            return false;
                        }

                        return true;
                    });

            if(sbFullyCovers.length()!=0){
                pairRDDRangeQuery = pairRDDRangeQuery.union(((JavaPairRDD<Void, TrajectorySegment>)jsc.newAPIHadoopFile(sbFullyCovers.toString(), ParquetInputFormat.class, Void.class, TrajectorySegment.class, job.getConfiguration())).filter(f-> ! ((f._2.getSpatioTemporalPoints().length>2 || !(f._2.getSegment()>1)) &&  HilbertUtil.isMinDistGreaterThan(f._2.getMinLongitude(), f._2.getMinLatitude(), f._2.getMaxLongitude(), f._2.getMaxLatitude(), trajectoryQuery, epsilon))));
            }

            pairRDDRangeQuery = pairRDDRangeQuery.groupBy(f->f._2.getObjectId())
                    .flatMapToPair(f->{

                        List<TrajectorySegment> trSegments = new ArrayList<>();
                        f._2.forEach(t->trSegments.add(t._2));

                        Comparator<TrajectorySegment> comparator = Comparator.comparingLong(d-> d.getSpatioTemporalPoints()[0].getTimestamp());
                        comparator = comparator.thenComparingLong(d-> Math.abs(d.getSegment()));

                        trSegments.sort(comparator);

                        if(trSegments.size()==1 && trSegments.get(0).getSegment()==-1){
                            return Collections.singletonList(new Tuple2<Void, TrajectorySegment>(null, trSegments.get(0))).iterator();
                        }

                        int segIndex=1;
                        for (int seg = 0; seg < trSegments.size()-1; seg++) {
                            if(trSegments.get(seg).getSegment()!=segIndex++){
                                return Collections.emptyIterator();
                            }
                        }

                        if(trSegments.get(trSegments.size()-1).getSegment()!=segIndex*(-1)){
                            return Collections.emptyIterator();
                        }

                        for (int i = 0; i < trSegments.size()-1; i++) {
                            SpatioTemporalPoint spatioTemporalPoint1 = trSegments.get(i).getSpatioTemporalPoints()[trSegments.get(i).getSpatioTemporalPoints().length-1];
                            SpatioTemporalPoint spatioTemporalPoint2 = trSegments.get(i+1).getSpatioTemporalPoints()[0];
                            if(!spatioTemporalPoint1.equals(spatioTemporalPoint2)){
                                throw new Exception("The ending starting points of continuous segments should be equal");
                            }
                        }
                        return Collections.singletonList(new Tuple2<Void, TrajectorySegment>(null, new TrajectorySegment(f._1,1, trSegments))).iterator();
                    })

                    .filter(f->{ if(Double.compare(HilbertUtil.euclideanDistance(f._2.getSpatioTemporalPoints()[0].getLongitude(),f._2.getSpatioTemporalPoints()[0].getLatitude(),trajectoryQuery[0].getLongitude(),trajectoryQuery[0].getLatitude()),epsilon)!=1
                            && Double.compare(HilbertUtil.euclideanDistance(f._2.getSpatioTemporalPoints()[f._2.getSpatioTemporalPoints().length-1].getLongitude(),f._2.getSpatioTemporalPoints()[f._2.getSpatioTemporalPoints().length-1].getLatitude(),trajectoryQuery[trajectoryQuery.length-1].getLongitude(),trajectoryQuery[trajectoryQuery.length-1].getLatitude()),epsilon)!=1) {
                            return true;
                        }else{
                            return false;
                        }
                    })

                    .filter(f->{if (f._2.getSpatioTemporalPoints().length>trajectoryQuery.length){
                            if(Double.compare(HilbertUtil.frechetDistance(trajectoryQuery, f._2.getSpatioTemporalPoints()),epsilon)!=1){
                                return true;
                            }else{
                                return false;
                            }
                        }else{
                            if(Double.compare(HilbertUtil.frechetDistance(f._2.getSpatioTemporalPoints(),trajectoryQuery),epsilon)!=1){
                                return true;
                            }else{
                                return false;
                            }
                        }
                    });



            List<Tuple2<Void,TrajectorySegment>> trajs = pairRDDRangeQuery.collect();
            long num = trajs.size();

            long numOfPoints = 0;
            for (Tuple2<Void, TrajectorySegment> voidTrajectoryTuple2 : trajs) {
                numOfPoints = numOfPoints + voidTrajectoryTuple2._2.getSpatioTemporalPoints().length;
            }
            System.out.println("Query is "+ Arrays.toString(trajectoryQuery));

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
