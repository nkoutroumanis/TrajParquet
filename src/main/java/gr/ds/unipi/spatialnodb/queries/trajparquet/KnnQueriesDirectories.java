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

public class KnnQueriesDirectories {
    public static void main(String args[]) throws IOException {

        Config config = loadConfig("queries-knn.conf");

        Config dataLoading = config.getConfig("queries");
        final String parquetPath = dataLoading.getString("parquetPath");
        final String queriesFilePath = dataLoading.getString("queriesFilePath");
        final String queriesFileExport = dataLoading.getString("queriesFileExport");
        final int k = dataLoading.getInt("k");

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

            final double queryMinLongitude = Double.max(minLon,mbrMinLongitude);
            final double queryMinLatitude = Double.max(minLat,mbrMinLatitude);
            final long queryMinTimestamp = minTime;

            final double queryMaxLongitude = Double.min(maxLon-0.0000001,mbrMaxLongitude);
            final double queryMaxLatitude = Double.min(maxLat-0.0000001,mbrMaxLatitude);
            final long queryMaxTimestamp = minTime;//maxTime-1000;

            long t1 = System.currentTimeMillis();

            long[] hilStart = HilbertUtil.scaleGeoTemporalPoint(queryMinLongitude, minLon, maxLon,queryMinLatitude, minLat, maxLat, queryMinTimestamp, minTime, maxTime, maxOrdinates);
            long[] hilEnd = HilbertUtil.scaleGeoTemporalPoint(queryMaxLongitude, minLon, maxLon, queryMaxLatitude, minLat, maxLat, queryMaxTimestamp, minTime, maxTime, maxOrdinates);
            Ranges ranges = hilbertCurve.query(hilStart, hilEnd, 0);
            final HashSet<Long> initialCubes = new HashSet<>();

            ranges.toList().forEach(range -> {
                for (long r = range.low(); r <= range.high(); r++) {
                        long[] cube = hilbertCurve.point(r);
                        double xMin = minLon + (cube[0] * (maxLon-minLon)/(maxOrdinates+ 1L));
                        double yMin = minLat + (cube[1] * (maxLat-minLat)/(maxOrdinates+ 1L));
//                        long tMin = minTime + (cube[2] * (maxTime-minTime)/(maxOrdinates+ 1L));

                        double xMax = minLon + ((cube[0]+1) * (maxLon-minLon)/(maxOrdinates+ 1L));
                        double yMax = minLat + ((cube[1]+1) * (maxLat-minLat)/(maxOrdinates+ 1L));
//                        long tMax = minTime + ((cube[2]+1) * (maxTime-minTime)/(maxOrdinates+ 1L));

                        if(doesTrajectoryIntersectWithCube(trajectoryQuery, xMin, yMin, xMax, yMax)){
                            initialCubes.add(r);
                        }
                }
            });

            StringBuilder sb = new StringBuilder();
            for (Long cubeId : initialCubes) {
                long[] cube = hilbertCurve.point(cubeId);
                for (long i = 0; i <= maxOrdinates; i++) {//TIME NEEDED
                    String j = String.valueOf(hilbertCurve.index(cube[0], cube[1], i));
                    if(directoriesSet.contains(j)) {
                        sb.append(parquetPath+"/"+j+",");
                    }
                }
            }
            sb.deleteCharAt(sb.length()-1);

            HashMap<String, List<TrajectorySegment>> identifiedTrajSegments = new HashMap<>();
            HashSet<String> flushedTrajectories = new HashSet<>();

            if(sb.length()!=0){
                JavaPairRDD<Void, TrajectorySegment> pairRDD = (JavaPairRDD<Void, TrajectorySegment>) jsc.newAPIHadoopFile(sb.toString(), ParquetInputFormat.class, Void.class, TrajectorySegment.class, job.getConfiguration());

                pairRDD.collect().forEach(seg->{
                    identifiedTrajSegments.compute(seg._2.getObjectId(), (i, v) -> {
                        if (v == null) {
                            List<TrajectorySegment> t = new ArrayList<>();
                            t.add(seg._2);
                            return t;
                        } else {
                            v.add(seg._2);
                            v.sort(Comparator.comparingLong(a -> Math.abs(a.getSegment())));
                            return v;
                        }
                    });
                });
                flush(identifiedTrajSegments, flushedTrajectories);
//                bw.write(0+";"+0+";"+0+";"+0);
//                DataPage.counter = 0;
//                bw.newLine();
//                continue;
            }

            BoundedPriorityQueue trajectoryQueue = BoundedPriorityQueue.newBoundedPriorityQueue(k);

            if(flushedTrajectories.size()>=k) {
                Iterator<String> it = flushedTrajectories.iterator();
                FilterPredicate filterPredicate = eq(binaryColumn("entityId"), Binary.fromCharSequence(flushedTrajectories.iterator().next()));
                while (it.hasNext()) {
                    filterPredicate = or(eq(binaryColumn("entityId"), Binary.fromCharSequence(flushedTrajectories.iterator().next())), filterPredicate);
                }
                ParquetInputFormat.setFilterPredicate(job.getConfiguration(), filterPredicate);
                JavaPairRDD<Void, TrajectorySegment> pairRDD = (JavaPairRDD<Void, TrajectorySegment>) jsc.newAPIHadoopFile(parquetPath + "/" + "trajectories", ParquetInputFormat.class, Void.class, TrajectorySegment.class, job.getConfiguration());
                List<TrajectoryScore> ts = pairRDD.map(t -> TrajectoryScore.newTrajectoryScore(t._2, HilbertUtil.frechetDistance(trajectoryQuery, t._2.getSpatioTemporalPoints()))).collect();
                ts.forEach(trajectoryQueue::add);
                flushedTrajectories.clear();
            }

            HashSet<Long> visitedCubes = new HashSet<>(initialCubes);
            PriorityQueue<CellScore> queueCells = new PriorityQueue<>(Comparator.comparingDouble(CellScore::getScore));

            visitedCubes.forEach(c-> {
                long[] cube = hilbertCurve.point(c);
                for (long i = Math.max(0, cube[0]-1); i <= Math.min(maxOrdinates-1, cube[0]+1); i++) {
                    for (long j = Math.max(0, cube[1]-1); j <= Math.min(maxOrdinates-1, cube[1]+1); j++) {
                        long cId = hilbertCurve.index(i,j,0);
                        if(!visitedCubes.contains(cId)){
                            long[] cubeNeighbor = hilbertCurve.point(cId);
                            queueCells.add(CellScore.newCellScore(cId, HilbertUtil.trajectoryMinDist(minLon + (cubeNeighbor[0] * (maxLon-minLon)/(maxOrdinates+ 1L)),minLat + (cubeNeighbor[1] * (maxLat-minLat)/(maxOrdinates+ 1L)),minLon + ((cubeNeighbor[0]+1) * (maxLon-minLon)/(maxOrdinates+ 1L)),minLat + ((cubeNeighbor[1]+1) * (maxLat-minLat)/(maxOrdinates+ 1L)),trajectoryQuery)));
                        }
                    }
                }
            });


            while( (trajectoryQueue.getSize() < k || queueCells.peek().getScore() < trajectoryQueue.getMaxScore()) && !queueCells.isEmpty()){

                long cell = queueCells.poll().getCellId();
                visitedCubes.add(cell);

                long[] cube = hilbertCurve.point(cell);
                for (long i = Math.max(0, cube[0]-1); i <= Math.min(maxOrdinates-1, cube[0]+1); i++) {
                    for (long j = Math.max(0, cube[1]-1); j <= Math.min(maxOrdinates-1, cube[1]+1); j++) {
                        long cId = hilbertCurve.index(i,j,0);
                        if(!visitedCubes.contains(cId)){
                            long[] cubeNeighbor = hilbertCurve.point(cId);
                            queueCells.add(CellScore.newCellScore(cId, HilbertUtil.trajectoryMinDist(minLon + (cubeNeighbor[0] * (maxLon-minLon)/(maxOrdinates+ 1L)),minLat + (cubeNeighbor[1] * (maxLat-minLat)/(maxOrdinates+ 1L)),minLon + ((cubeNeighbor[0]+1) * (maxLon-minLon)/(maxOrdinates+ 1L)),minLat + ((cubeNeighbor[1]+1) * (maxLat-minLat)/(maxOrdinates+ 1L)),trajectoryQuery)));
                        }
                    }
                }

                sb.setLength(0);

                for (long i = 0; i <= maxOrdinates; i++) {//TIME NEEDED
                    String j = String.valueOf(hilbertCurve.index(cube[0], cube[1], i));
                    if(directoriesSet.contains(j)) {
                        sb.append(parquetPath+"/"+j+",");
                    }
                }
                sb.deleteCharAt(sb.length()-1);


                if(sb.length()!=0){
                    JavaPairRDD<Void, TrajectorySegment> pairRDD = (JavaPairRDD<Void, TrajectorySegment>) jsc.newAPIHadoopFile(sb.toString(), ParquetInputFormat.class, Void.class, TrajectorySegment.class, job.getConfiguration());
                    pairRDD.collect().forEach(seg->{
                        identifiedTrajSegments.compute(seg._2.getObjectId(), (i, v) -> {
                            if (v == null) {
                                List<TrajectorySegment> t = new ArrayList<>();
                                t.add(seg._2);
                                return t;
                            } else {
                                v.add(seg._2);
                                v.sort(Comparator.comparingLong(a -> Math.abs(a.getSegment())));
                                return v;
                            }
                        });
                    });
                    flush(identifiedTrajSegments, flushedTrajectories);
                }

                if(!flushedTrajectories.isEmpty() && flushedTrajectories.size()>= (k-trajectoryQueue.getSize())){
                    Iterator<String> it = flushedTrajectories.iterator();
                    FilterPredicate filterPredicate = eq(binaryColumn("entityId"), Binary.fromCharSequence(flushedTrajectories.iterator().next()));
                    while (it.hasNext()) {
                        filterPredicate = or(eq(binaryColumn("entityId"), Binary.fromCharSequence(flushedTrajectories.iterator().next())), filterPredicate);
                    }
                    ParquetInputFormat.setFilterPredicate(job.getConfiguration(), filterPredicate);
                    JavaPairRDD<Void, TrajectorySegment> pairRDD = (JavaPairRDD<Void, TrajectorySegment>) jsc.newAPIHadoopFile(parquetPath + "/" + "trajectories", ParquetInputFormat.class, Void.class, TrajectorySegment.class, job.getConfiguration());
                    List<TrajectoryScore> ts = pairRDD.map(t -> TrajectoryScore.newTrajectoryScore(t._2, HilbertUtil.frechetDistance(trajectoryQuery, t._2.getSpatioTemporalPoints()))).collect();
                    ts.forEach(trajectoryQueue::add);
                    flushedTrajectories.clear();
                }
            }

            long t2 = System.currentTimeMillis();

            long startTime = System.currentTimeMillis();

//            List<Tuple2<Void,TrajectorySegment>> trajs = pairRDD.collect();
            long num =trajectoryQueue.getSize();// trajs.size();

            long numOfPoints = 0;

//            for (Tuple2<Void, TrajectorySegment> voidTrajectoryTuple2 : trajs) {
//                numOfPoints = numOfPoints + voidTrajectoryTuple2._2.getSpatioTemporalPoints().length;
//            }
            System.out.println("Query is "+ Arrays.toString(trajectoryQuery));
            long endTime = System.currentTimeMillis();
            times.add((endTime - startTime));

            bw.write((endTime - startTime)+";"+num+";"+numOfPoints+";"+ DataPage.counter+";"+(t2-t1));
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
