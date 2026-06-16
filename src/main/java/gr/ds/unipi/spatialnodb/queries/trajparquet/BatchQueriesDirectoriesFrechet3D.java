package gr.ds.unipi.spatialnodb.queries.trajparquet;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.messages.common.IndexUtils;
import gr.ds.unipi.spatialnodb.messages.common.IndexUtils2D;
import gr.ds.unipi.spatialnodb.messages.common.IndexUtils3D;
import gr.ds.unipi.spatialnodb.messages.common.SpatioTemporalPoint;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.TrajectorySegment;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.TrajectorySegmentPartialReadSupport;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.TrajectorySegmentReadSupport;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.TrajectorySegmentWithMetadata;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.pathReadParquet.ParquetInputFormatWithKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.io.api.Binary;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
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
import static gr.ds.unipi.spatialnodb.dataloading.HilbertUtil.doesTrajectoryIntersectWithCube;
import static gr.ds.unipi.spatialnodb.dataloading.HilbertUtil.isTrajectoryDistanceLessThanEpsilonToCube;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.in;

public class BatchQueriesDirectoriesFrechet3D {
    public static void main(String args[]) throws IOException {

        Config config = loadConfig("queries.conf");
        final int partitions = Integer.parseInt(args[0]);

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
                        new java.io.InputStreamReader(in)
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

        Job jobWholeTrajectory = Job.getInstance();
        Job jobTrajectorySegments = Job.getInstance();

        ParquetInputFormat.setReadSupportClass(jobWholeTrajectory, TrajectorySegmentReadSupport.class);
        ParquetInputFormat.setReadSupportClass(jobTrajectorySegments, TrajectorySegmentPartialReadSupport.class);

        SparkConf sparkConf = new SparkConf().registerKryoClasses(new Class[]{SpatioTemporalPoint.class,SpatioTemporalPoint[].class, HilbertUtil.class});
        sparkConf.setAppName("Batch Similarity Querying in TrajParquet");
        if (!sparkConf.contains("spark.master")) {
            sparkConf.setMaster("local[*]").set("spark.executor.memory","4g");
        }
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        long startTime = System.currentTimeMillis();

        Broadcast br =  jsc.broadcast(hilbertCurve);

        Set<String> directoriesSet = new HashSet<>();
        if(parquetPath.startsWith("hdfs://")){
            Path stIndexPath = new Path(parquetPath + "/stIndex");
            FileSystem fs = stIndexPath.getFileSystem(jobWholeTrajectory.getConfiguration());
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

        Function<String, TrajectorySegment> readTrajectoriesFunction = new Function<String, TrajectorySegment>() {
            @Override
            public TrajectorySegment call(String query) {

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
                return new TrajectorySegment("", -1, trajectoryQuery, mbrMinLongitude, mbrMinLatitude, mbrMinTimestamp, mbrMaxLongitude, mbrMaxLatitude, mbrMaxTimestamp);            }
        };


        PairFlatMapFunction<Tuple2<Long, TrajectorySegment>, Long, Long> cubeToQueryIdsFunction = tuple2 -> {

            SmallHilbertCurve hilbertCurveBr = ((SmallHilbertCurve) br.getValue());

            final double queryMinLongitude = Double.max(minLon,tuple2._2.getMinLongitude()-epsilon);
            final double queryMinLatitude = Double.max(minLat,tuple2._2.getMinLatitude()-epsilon);
            final long queryMinTimestamp = minTime;

            final double queryMaxLongitude = Double.min(maxLon-0.0000001,tuple2._2.getMaxLongitude()+epsilon);
            final double queryMaxLatitude = Double.min(maxLat-0.0000001,tuple2._2.getMaxLatitude()+epsilon);
            final long queryMaxTimestamp = maxTime-1000;

            long[] hilStart = indexUtils.scale(queryMinLongitude, queryMinLatitude, queryMinTimestamp);//HilbertUtil.scaleGeoTemporalPoint(queryMinLongitude, minLon, maxLon,queryMinLatitude, minLat, maxLat, queryMinTimestamp, minTime, maxTime, maxOrdinates);
            long[] hilEnd = indexUtils.scale(queryMaxLongitude, queryMaxLatitude, queryMaxTimestamp);//HilbertUtil.scaleGeoTemporalPoint(queryMaxLongitude, minLon, maxLon, queryMaxLatitude, minLat, maxLat, queryMaxTimestamp, minTime, maxTime, maxOrdinates);
            Ranges ranges = hilbertCurveBr.query(hilStart, hilEnd, 0);

            List<Tuple2<Long,Long>> trajectoryIdToCubes = new ArrayList<>();

            if(indexUtils instanceof IndexUtils2D){
                for (Range range : ranges.toList()) {
                    for (long r = range.low(); r <= range.high(); r++) {
                        if(directoriesSet.contains(String.valueOf(r))) {
                            long[] cube = hilbertCurveBr.point(r);
                            double xMin = minLon + (cube[0] * (maxLon-minLon)/(maxOrdinates+ 1L));
                            double yMin = minLat + (cube[1] * (maxLat-minLat)/(maxOrdinates+ 1L));

                            double xMax = minLon + ((cube[0]+1) * (maxLon-minLon)/(maxOrdinates+ 1L));
                            double yMax = minLat + ((cube[1]+1) * (maxLat-minLat)/(maxOrdinates+ 1L));

                            if(doesTrajectoryIntersectWithCube(tuple2._2.getSpatioTemporalPoints(), xMin, yMin, xMax, yMax) || isTrajectoryDistanceLessThanEpsilonToCube(tuple2._2.getSpatioTemporalPoints(), xMin, yMin, xMax, yMax,epsilon)){
                                trajectoryIdToCubes.add(Tuple2.apply(r,tuple2._1));
                            }
                        }
                    }
                }
            }else{
                for (long i = hilStart[0]; i <= hilEnd[0]; i++) {
                    for (long j = hilStart[1]; j <= hilEnd[1]; j++) {
                            double xMin = minLon + (i * (maxLon - minLon) / (maxOrdinates + 1L));
                            double yMin = minLat + (j * (maxLat - minLat) / (maxOrdinates + 1L));

                            double xMax = minLon + ((i + 1) * (maxLon - minLon) / (maxOrdinates + 1L));
                            double yMax = minLat + ((j + 1) * (maxLat - minLat) / (maxOrdinates + 1L));

                            if(doesTrajectoryIntersectWithCube(tuple2._2.getSpatioTemporalPoints(), xMin, yMin, xMax, yMax) || isTrajectoryDistanceLessThanEpsilonToCube(tuple2._2.getSpatioTemporalPoints(), xMin, yMin, xMax, yMax,epsilon)) {
                                for (long k = hilStart[2]; k <= hilEnd[2]; k++) {
                                    long cubeId = hilbertCurveBr.index(i,j,k);
                                    if(directoriesSet.contains(String.valueOf(cubeId))) {
                                        trajectoryIdToCubes.add(Tuple2.apply(cubeId,tuple2._1));
                                    }
                                }
                            }
                    }
                }
            }

//            final HashSet<Long> initialCubes = new HashSet<>();
//
//            ranges.toList().forEach(range -> {
//                for (long r = range.low(); r <= range.high(); r++) {
//                    long[] cube = hilbertCurveBr.point(r);
//                    double xMin = minLon + (cube[0] * (maxLon-minLon)/(maxOrdinates+ 1L));
//                    double yMin = minLat + (cube[1] * (maxLat-minLat)/(maxOrdinates+ 1L));
////                        long tMin = minTime + (cube[2] * (maxTime-minTime)/(maxOrdinates+ 1L));
//
//                    double xMax = minLon + ((cube[0]+1) * (maxLon-minLon)/(maxOrdinates+ 1L));
//                    double yMax = minLat + ((cube[1]+1) * (maxLat-minLat)/(maxOrdinates+ 1L));
////                        long tMax = minTime + ((cube[2]+1) * (maxTime-minTime)/(maxOrdinates+ 1L));
//
//                    if(doesTrajectoryIntersectWithCube(tuple2._2.getSpatioTemporalPoints(), xMin, yMin, xMax, yMax) || isTrajectoryDistanceLessThanEpsilonToCube(tuple2._2.getSpatioTemporalPoints(), xMin, yMin, xMax, yMax,epsilon)){
//                        initialCubes.add(r);
//                    }
//                }
//            });
//
//            List<Tuple2<Long,Long>> trajectoryIdToCubes = new ArrayList<>(initialCubes.size());
//            for (Long cubeId : initialCubes) {
//                long[] cube = hilbertCurveBr.point(cubeId);
//                for (long i = 0; i <= maxOrdinates; i++) {
//                    long c = hilbertCurveBr.index(cube[0], cube[1], i);
//                    String j = String.valueOf(c);
//                    if(directoriesSet.contains(j)) {
//                        trajectoryIdToCubes.add(Tuple2.apply(c,tuple2._1));
//                    }
//                }
//            }
            return trajectoryIdToCubes.iterator();
        };


        JavaPairRDD<Long,TrajectorySegment> trajectoryQueries = jsc.textFile(queriesFilePath).map(readTrajectoriesFunction).zipWithUniqueId().mapToPair(i-> Tuple2.apply(i._2, i._1)).cache();
        JavaPairRDD<Long,Long> cubeToQueryId = trajectoryQueries.<Long,Long>flatMapToPair(cubeToQueryIdsFunction);


        List<Long> cubes = cubeToQueryId.keys().distinct().collect();
        StringBuilder sb = new StringBuilder();
        cubes.forEach(cube -> sb.append(parquetPath+File.separator+"stIndex").append(File.separator).append(cube).append(","));
        sb.deleteCharAt(sb.length()-1);

        JavaPairRDD<Long, TrajectorySegmentWithMetadata> pairRDD = (JavaPairRDD<Long, TrajectorySegmentWithMetadata>) jsc.newAPIHadoopFile(sb.toString(), ParquetInputFormatWithKey.class, Long.class, TrajectorySegmentWithMetadata.class, jobTrajectorySegments.getConfiguration());

        JavaPairRDD<String, Long> objectIdQueryIdPairs = pairRDD.join(cubeToQueryId, partitions).mapToPair((m->{return Tuple2.apply(Tuple2.apply(m._2._1.getTrajectorySegment().getObjectId(), m._2._2), m._2._1.getTrajectorySegment().getSegment());})).groupByKey().filter(f-> {
            List<Long> segsIds = new ArrayList<>((int) f._2.spliterator().getExactSizeIfKnown());
            f._2.forEach(segsIds::add);

            segsIds.sort(Comparator.comparingLong(Math::abs));

            if (segsIds.size() == 1 && segsIds.get(0) == -1) {
                return true;
            }

            long segIndex = 1;
            for (int seg = 0; seg < segsIds.size() - 1; seg++) {
                if (segsIds.get(seg) != segIndex++) {
                    return false;
                }
            }

            if(segsIds.get(segsIds.size()-1)!=segIndex*(-1)){
                return false;
            }

            return true;

        }).<String,Long>mapToPair(i->Tuple2.apply(i._1._1, i._1._2));

        List<String> objectIds = objectIdQueryIdPairs.keys().distinct().collect();

        Set<Binary> oIds = new HashSet<>(objectIds.size());
        objectIds.forEach(oId -> oIds.add(Binary.fromString(oId)));
        FilterPredicate filterPredicate = in(binaryColumn("objectId"), oIds);

        ParquetInputFormat.setFilterPredicate(jobWholeTrajectory.getConfiguration(), filterPredicate);

        JavaPairRDD<Void, TrajectorySegment> trajectories = (JavaPairRDD<Void, TrajectorySegment>) jsc.newAPIHadoopFile(parquetPath+File.separator+"idIndex", ParquetInputFormat.class, Void.class, TrajectorySegment.class, jobWholeTrajectory.getConfiguration());
        JavaPairRDD<Long, Tuple2<TrajectorySegment,TrajectorySegment>> results = trajectories.mapToPair(i-> Tuple2.apply(i._2.getObjectId(), i._2)).join(objectIdQueryIdPairs, partitions).mapToPair(i-> Tuple2.apply(i._2._2, i._2._1)).join(trajectoryQueries, partitions)

                .filter(f->{ if(Double.compare(HilbertUtil.euclideanDistance(f._2._1.getSpatioTemporalPoints()[0].getLongitude(),f._2._1.getSpatioTemporalPoints()[0].getLatitude(),   f._2._2.getSpatioTemporalPoints()[0].getLongitude(),f._2._2.getSpatioTemporalPoints()[0].getLatitude()),epsilon)!=1
                        && Double.compare(HilbertUtil.euclideanDistance(f._2._1.getSpatioTemporalPoints()[f._2._1.getSpatioTemporalPoints().length-1].getLongitude(),f._2._1.getSpatioTemporalPoints()[f._2._1.getSpatioTemporalPoints().length-1].getLatitude(), f._2._2.getSpatioTemporalPoints()[f._2._2.getSpatioTemporalPoints().length-1].getLongitude(),f._2._2.getSpatioTemporalPoints()[f._2._2.getSpatioTemporalPoints().length-1].getLatitude()                      ),epsilon)!=1) {
                    return true;
                }else{
                    return false;
                }})
                .filter(f->{
                    if(Double.compare(HilbertUtil.frechetDistance(f._2._1.getSpatioTemporalPoints(), f._2._2.getSpatioTemporalPoints()),epsilon)!=1){
                        return true;
                    }else{
                        return false;
                    }
                });

        System.out.println(results.count());
//        results.collect().forEach(i->{
//            System.out.println(i._1+" - "+i._2._2.getObjectId());
//        });
        long endTime = System.currentTimeMillis();

        BufferedWriter bw = new BufferedWriter(new FileWriter(metricsPath+ File.separator+"metrics-batch-frechet-queries-"+Paths.get(parquetPath).getFileName().toString()+"-"+ Paths.get(queriesFilePath).getFileName().toString().replaceFirst("\\.[^.]+$", "")+".txt"));
        bw.write("Total Time (ms)\tTotal Pages\n");
        bw.write((endTime-startTime) + "\t"+ DataPage.counter);
        bw.close();

        sparkSession.close();
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
