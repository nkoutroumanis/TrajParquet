package gr.ds.unipi.spatialnodb.queries.trajparquet;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import gr.ds.unipi.spatialnodb.SparkLogParser;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.messages.common.*;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.Range;
import org.davidmoten.hilbert.Ranges;
import org.davidmoten.hilbert.SmallHilbertCurve;
import scala.Tuple2;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;

import static gr.ds.unipi.spatialnodb.AppConfig.loadConfig;
import static gr.ds.unipi.spatialnodb.dataloading.HilbertUtil.areTrajectoryPointsDistanceLessThanEpsilonToCube;
import static org.apache.parquet.filter2.predicate.FilterApi.*;

public class Frechet3DQueriesDirectoriesIntervalsPivots {
    public static void main(String args[]) throws IOException {

        Config config = loadConfig("queries.conf");

        Config dataLoading = config.getConfig("queries");
        final String parquetPath = dataLoading.getString("parquetPath");
        final String queriesFilePath = dataLoading.getString("queriesFilePath");
        final String metricsPath = dataLoading.getString("metricsPath");
        final double epsilon = dataLoading.getDouble("epsilon");

        Config metadata;
        if(parquetPath.startsWith("hdfs://")){
            Path filePath = new Path(parquetPath, "space.metadata");
            FileSystem fs = filePath.getFileSystem(new Configuration());
            try (InputStream in = fs.open(filePath)) {
                metadata = ConfigFactory.parseReader(
                        new InputStreamReader(in)
                ).resolve().getConfig("gridHilbert");
            }
        }else{
            metadata = ConfigFactory.parseFile(new File(parquetPath+ File.separator+"space.metadata")).resolve().getConfig("gridHilbert");
        }
        final int bits = metadata.getInt("bits");
        Config boundaries = metadata.getConfig("boundaries");
        final double minLon = boundaries.getDouble("minLon");
        final double minLat = boundaries.getDouble("minLat");
        final long minTime = boundaries.getLong("minTime");
        final double maxLon = boundaries.getDouble("maxLon");
        final double maxLat = boundaries.getDouble("maxLat");
        final long maxTime = boundaries.getLong("maxTime");
        final String indexType = metadata.getString("indexType");

        final SmallHilbertCurve hilbertCurve = HilbertCurve.small().bits(bits).dimensions(indexType.equals("3D")?3:2);
        final long maxOrdinates = hilbertCurve.maxOrdinate();

        final IndexUtils indexUtils;
        if(!(indexType.equals("2D") || indexType.equals("3D"))) {
            throw new IllegalArgumentException("The index parameter must be either 2D or 3D");
        }
        if(indexType.equals("3D")) {
            indexUtils = new IndexUtils3D(minLon, minLat, minTime, maxLon, maxLat, maxTime, maxOrdinates);
        }else {
            indexUtils = new IndexUtils2D(minLon, minLat, maxLon, maxLat, maxOrdinates);
        }

        Job jobIntersected = Job.getInstance();
        Job jobFullyContains = Job.getInstance();

        ParquetInputFormat.setReadSupportClass(jobIntersected, TrajectorySegmentWithMetadataReadSupport.class);
        ParquetInputFormat.setReadSupportClass(jobFullyContains, TrajectorySegmentWithMetadataReadSupport.class);

        SparkConf sparkConf = new SparkConf();//.registerKryoClasses(new Class[]{SpatioTemporalPoint.class,SpatioTemporalPoint[].class});/*.setMaster("local[1]").set("spark.executor.memory","1g")*/
        sparkConf.setAppName("Similarity Querying in TrajParquet");
        if (!sparkConf.contains("spark.master")) {
            sparkConf.setMaster("local[*]").set("spark.executor.memory","4g");
        }
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        Set<String> directoriesSet = new HashSet<>();
        if(parquetPath.startsWith("hdfs://")){
            Path stIndexPath = new Path(parquetPath + "/stIndex");
            FileSystem fs = stIndexPath.getFileSystem(jobIntersected.getConfiguration());
            FileStatus[] statuses = fs.listStatus(stIndexPath);

            for (FileStatus status : statuses) {
                if (status.isDirectory()) {
                    directoriesSet.add(status.getPath().getName());
                }
            }
        }else{
            File[] directories = new File(parquetPath+ File.separator+"stIndex").listFiles(File::isDirectory);
            for (File directory : directories) {
                directoriesSet.add(directory.getName());
            }
        }

        BufferedWriter bw = new BufferedWriter(new FileWriter(metricsPath+ File.separator+"frechet-queries-optimized-"+Paths.get(parquetPath).getFileName().toString()+"-"+ Paths.get(queriesFilePath).getFileName().toString().replaceFirst("\\.[^.]+$", "")+".txt"));
        BufferedReader br = new BufferedReader(new FileReader(queriesFilePath));
        bw.write("Time Exec\tNum of Trajectories\tNum of Points\tIssued\tData Pages\tParse\n");
        String query;
        while ((query = br.readLine()) != null) {
            long startTime = System.currentTimeMillis();

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

            final double queryMinLongitude = Double.max(minLon,mbrMinLongitude-epsilon);
            final double queryMinLatitude = Double.max(minLat,mbrMinLatitude-epsilon);
            final long queryMinTimestamp = minTime;

            final double queryMaxLongitude = Double.min(maxLon-0.0000001,mbrMaxLongitude+epsilon);
            final double queryMaxLatitude = Double.min(maxLat-0.0000001,mbrMaxLatitude+epsilon);
            final long queryMaxTimestamp = maxTime-1000;//minTime;//maxTime-1000;

            FilterPredicate xAxis = and(gtEq(doubleColumn("maxLongitude"), queryMinLongitude), ltEq(doubleColumn("minLongitude"), queryMaxLongitude));
            FilterPredicate yAxis = and(gtEq(doubleColumn("maxLatitude"), queryMinLatitude), ltEq(doubleColumn("minLatitude"), queryMaxLatitude));
            FilterPredicate tAxis = and(gtEq(longColumn("maxTimestamp"), queryMinTimestamp), ltEq(longColumn("minTimestamp"), queryMaxTimestamp));
            FilterPredicate notNullInterval = notEq(longColumn("intervalStart"), null);

            ParquetInputFormat.setFilterPredicate(jobIntersected.getConfiguration(), and(and(tAxis, and(xAxis, yAxis)), notNullInterval));

            long[] hilStart = indexUtils.scale(queryMinLongitude, queryMinLatitude, queryMinTimestamp);//HilbertUtil.scaleGeoTemporalPoint(queryMinLongitude, minLon, maxLon,queryMinLatitude, minLat, maxLat, queryMinTimestamp, minTime, maxTime, maxOrdinates);
            long[] hilEnd = indexUtils.scale(queryMaxLongitude, queryMaxLatitude, queryMaxTimestamp);//HilbertUtil.scaleGeoTemporalPoint(queryMaxLongitude, minLon, maxLon, queryMaxLatitude, minLat, maxLat, queryMaxTimestamp, minTime, maxTime, maxOrdinates);
            Ranges ranges = hilbertCurve.query(hilStart, hilEnd, 0);
            StringBuilder sbFullyCovers = new StringBuilder();
            StringBuilder sbIntersected = new StringBuilder();

            if(indexUtils instanceof IndexUtils2D){
                boolean flag = false;
                for (Range range : ranges.toList()) {
                    for (long r = range.low(); r <= range.high(); r++) {
                        if(directoriesSet.contains(String.valueOf(r))) {
                            long[] cube = hilbertCurve.point(r);
                            double xMin = minLon + (cube[0] * (maxLon-minLon)/(maxOrdinates+ 1L));
                            double yMin = minLat + (cube[1] * (maxLat-minLat)/(maxOrdinates+ 1L));

                            double xMax = minLon + ((cube[0]+1) * (maxLon-minLon)/(maxOrdinates+ 1L));
                            double yMax = minLat + ((cube[1]+1) * (maxLat-minLat)/(maxOrdinates+ 1L));

                            if(areTrajectoryPointsDistanceLessThanEpsilonToCube(trajectoryQuery, xMin, yMin, xMax, yMax,epsilon)){
                                for (int i = 0; i < cube.length; i++) {
                                    if(cube[i] == hilStart[i] || cube[i] == hilEnd[i]) {
                                        flag = true;
                                        break;
                                    }
                                }
                                if(flag) {
                                    sbIntersected.append(parquetPath+ File.separator+"stIndex"+File.separator+r+",");
                                }else{
                                    sbFullyCovers.append(parquetPath+ File.separator+"stIndex"+File.separator+r+",");
                                }
                                flag = false;
                            }
                        }
                    }
                }
            }else{
                for (long i = hilStart[0]; i <= hilEnd[0]; i++) {
                    for (long j = hilStart[1]; j <= hilEnd[1]; j++) {
                        double xMin = minLon + (i * (maxLon-minLon)/(maxOrdinates+ 1L));
                        double yMin = minLat + (j * (maxLat-minLat)/(maxOrdinates+ 1L));

                        double xMax = minLon + ((i+1) * (maxLon-minLon)/(maxOrdinates+ 1L));
                        double yMax = minLat + ((j+1) * (maxLat-minLat)/(maxOrdinates+ 1L));

                        if(areTrajectoryPointsDistanceLessThanEpsilonToCube(trajectoryQuery, xMin, yMin, xMax, yMax,epsilon)) {
                            for (long k = hilStart[2]; k <= hilEnd[2]; k++) {
                                long cubeId = hilbertCurve.index(i,j,k);
                                if(directoriesSet.contains(String.valueOf(cubeId))) {
                                    if(i==hilStart[0] || i==hilEnd[0] || j==hilStart[1] || j==hilEnd[1] || k==hilStart[2] || k==hilEnd[2]){
                                        sbIntersected.append(parquetPath+ File.separator+"stIndex"+File.separator+cubeId+",");
                                    }else{
                                        sbFullyCovers.append(parquetPath+ File.separator+"stIndex"+File.separator+cubeId+",");
                                    }
                                }
                            }
                        }
                    }
                }
            }
            long parseAndCubeIndex = System.currentTimeMillis() - startTime;

            if(sbIntersected.length()==0 && sbFullyCovers.length()==0){
                long endTime = System.currentTimeMillis();
                bw.write((endTime-startTime)+"\t"+0+"\t"+0+"\t"+"false"+"\t"+DataPage.counter+"\t"+parseAndCubeIndex);
                DataPage.counter = 0;
                bw.newLine();
                continue;
            }

            if(sbFullyCovers.length() != 0){
                sbFullyCovers.deleteCharAt(sbFullyCovers.length()-1);
            }
            sbIntersected.deleteCharAt(sbIntersected.length()-1);

            JavaPairRDD<Void, TrajectorySegmentWithMetadata> pairRDDRangeQuery = (JavaPairRDD<Void, TrajectorySegmentWithMetadata>) jsc.newAPIHadoopFile(sbIntersected.toString(), ParquetInputFormat.class, Void.class, TrajectorySegmentWithMetadata.class, jobIntersected.getConfiguration());
            pairRDDRangeQuery = pairRDDRangeQuery.filter(f -> {
                for (SpatialPoint pivot : f._2.getPivots()) {
                    if(HilbertUtil.isPointMinDistGreaterThan(pivot.getLongitude(), pivot.getLatitude(), trajectoryQuery,epsilon)){
                        return false;
                    }
                }
                return true;
            });

            if(sbFullyCovers.length()!=0){
                if(indexUtils instanceof IndexUtils2D){
                    ParquetInputFormat.setFilterPredicate(jobFullyContains.getConfiguration(), and(tAxis, notNullInterval));
                }else{
                    ParquetInputFormat.setFilterPredicate(jobFullyContains.getConfiguration(), notNullInterval);
                }

                JavaPairRDD<Void, TrajectorySegmentWithMetadata> fullyContainedPairRDD = (JavaPairRDD<Void, TrajectorySegmentWithMetadata>) jsc.newAPIHadoopFile(sbFullyCovers.toString(), ParquetInputFormat.class, Void.class, TrajectorySegmentWithMetadata.class, jobFullyContains.getConfiguration());
                fullyContainedPairRDD = fullyContainedPairRDD.filter(f -> {
                    for (SpatialPoint pivot : f._2.getPivots()) {
                        if(HilbertUtil.isPointMinDistGreaterThan(pivot.getLongitude(), pivot.getLatitude(), trajectoryQuery,epsilon)){
                            return false;
                        }
                    }
                    return true;
                });
                if(indexUtils instanceof IndexUtils2D){
                    fullyContainedPairRDD = fullyContainedPairRDD.filter(f->{if(Long.compare(f._2.getTrajectorySegment().getMinTimestamp(), queryMinTimestamp)==-1 || Long.compare(f._2.getTrajectorySegment().getMaxTimestamp(), queryMaxTimestamp)==1){return false;} return true;});
                }

                pairRDDRangeQuery = pairRDDRangeQuery.union(fullyContainedPairRDD);
            }

            JavaPairRDD<Void, TrajectorySegment> results = pairRDDRangeQuery.groupBy(f->f._2.getTrajectorySegment().getObjectId(), Integer.parseInt(args[0]))
                    .flatMapToPair(f->{
                        List<TrajectorySegmentWithMetadata> trSegments = new ArrayList<>();
                        f._2.forEach(t->trSegments.add(t._2));

//                        Comparator<TrajectorySegmentWithIntervalMetadata> comparator = Comparator.comparingLong(d-> d.getTrajectorySegment().getSpatioTemporalPoints()[0].getTimestamp());
//                        comparator = comparator.thenComparingDouble(d-> d.getTrajectorySegment().getSpatioTemporalPoints()[0].getLongitude());
//                        comparator = comparator.thenComparingDouble(d-> d.getTrajectorySegment().getSpatioTemporalPoints()[0].getLatitude());
                        Comparator<TrajectorySegmentWithMetadata> comparator = Comparator.comparingLong(d-> d.getInterval()[0]);

                        trSegments.sort(comparator);


                        if(trSegments.size()==1 && trSegments.get(0).getInterval()[0]==1 && trSegments.get(0).getInterval()[1]<0){
                            return Collections.singletonList(new Tuple2<Void, TrajectorySegment>(null, trSegments.get(0).getTrajectorySegment())).iterator();
                        }

                        long y;
                        if(trSegments.get(0).getInterval()[0]!=1 || trSegments.get(trSegments.size()-1).getInterval()[1]>0){
                            return Collections.emptyIterator();
                        }else{
                            y = trSegments.get(0).getInterval()[1];
                        }
                        for (int i = 1; i < trSegments.size()-1; i++) {
                                if(y+1 != trSegments.get(i).getInterval()[0]) {return Collections.emptyIterator();}
                                y = trSegments.get(i).getInterval()[1];
                        }
                        if(y+1!=trSegments.get(trSegments.size()-1).getInterval()[0]){return Collections.emptyIterator();}

                        List<TrajectorySegment> ts = new ArrayList<>(trSegments.size());
                        trSegments.forEach(e->ts.add(e.getTrajectorySegment()));

                        return Collections.singletonList(new Tuple2<Void, TrajectorySegment>(null, new TrajectorySegment(f._1,1, ts))).iterator();
                    })
                    .filter(f->{ if(Double.compare(HilbertUtil.euclideanDistance(f._2.getSpatioTemporalPoints()[0].getLongitude(),f._2.getSpatioTemporalPoints()[0].getLatitude(),trajectoryQuery[0].getLongitude(),trajectoryQuery[0].getLatitude()),epsilon)!=1
                            && Double.compare(HilbertUtil.euclideanDistance(f._2.getSpatioTemporalPoints()[f._2.getSpatioTemporalPoints().length-1].getLongitude(),f._2.getSpatioTemporalPoints()[f._2.getSpatioTemporalPoints().length-1].getLatitude(),trajectoryQuery[trajectoryQuery.length-1].getLongitude(),trajectoryQuery[trajectoryQuery.length-1].getLatitude()),epsilon)!=1) {
                            return true;
                        }else{
                            return false;
                        }
                    })
                    .filter(f->{
                            if(Double.compare(HilbertUtil.frechetDistance(trajectoryQuery, f._2.getSpatioTemporalPoints()),epsilon)!=1){
                                return true;
                            }else{
                                return false;
                            }
                    });

            List<Tuple2<Void,TrajectorySegment>> trajs = results.collect();
            long endTime = System.currentTimeMillis();
            long num = trajs.size();

            long numOfPoints = 0;
            for (Tuple2<Void, TrajectorySegment> voidTrajectoryTuple2 : trajs) {
                numOfPoints = numOfPoints + voidTrajectoryTuple2._2.getSpatioTemporalPoints().length;
            }

            bw.write((endTime - startTime)+"\t"+num+"\t"+numOfPoints+"\t"+"true"+"\t"+DataPage.counter+"\t"+parseAndCubeIndex);
            DataPage.counter = 0;
            bw.newLine();
        }
        bw.close();
        br.close();

        String applicationId = sparkSession.sparkContext().applicationId();

        sparkSession.close();

        if(sparkConf.getBoolean("spark.eventLog.enabled",false)){
            String eventLogDir = sparkConf.get("spark.eventLog.dir");
            File dir = new File(eventLogDir.replace("file:", ""));
            File eventLogFile =
                    Arrays.stream(dir.listFiles())
                            .filter(File::isFile)
                            .filter(f -> f.getName().contains(applicationId))
                            .findFirst()
                            .orElseThrow(() -> new IllegalStateException(
                                    "No event log file found for application " + applicationId));

            List<Long>[] lists = SparkLogParser.getTimeFromTwoStagesPerJob(eventLogFile.getAbsolutePath());
            try {
                SparkLogParser.enrichQueryAdHocFile(metricsPath+ File.separator+"frechet-queries-optimized-intervals-"+Paths.get(parquetPath).getFileName().toString()+"-"+ Paths.get(queriesFilePath).getFileName().toString().replaceFirst("\\.[^.]+$", "")+".txt", lists);
            }catch (Exception e) {
                e.printStackTrace();
            }
            eventLogFile.delete();
        }
    }

    private static int countPoints(String line) {
        int count = 0;
        for (int i = 0; i < line.length(); i++) {
            if (line.charAt(i) == ';') count++;
        }
        return count;
    }

//    // Parse a double directly from char[] between start (inclusive) and end (exclusive)
//    private static double parseDouble(char[] chars, int start, int end) {
//        double result = 0;
//        boolean neg = false;
//        int i = start;
//        if (chars[i] == '-') { neg = true; i++; }
//
//        // Integer part
//        while (i < end && chars[i] >= '0' && chars[i] <= '9') {
//            result = result * 10 + (chars[i] - '0');
//            i++;
//        }
//
//        // Fractional part
//        if (i < end && chars[i] == '.') {
//            i++;
//            double frac = 0;
//            double div = 1;
//            while (i < end && chars[i] >= '0' && chars[i] <= '9') {
//                frac = frac * 10 + (chars[i] - '0');
//                div *= 10;
//                i++;
//            }
//            result += frac / div;
//        }
//
//        return neg ? -result : result;
//    }
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
