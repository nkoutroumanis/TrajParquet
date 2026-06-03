package gr.ds.unipi.spatialnodb.queries.trajparquet;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.messages.common.*;
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
import org.davidmoten.hilbert.Range;
import org.davidmoten.hilbert.Ranges;
import org.davidmoten.hilbert.SmallHilbertCurve;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;

import static gr.ds.unipi.spatialnodb.AppConfig.loadConfig;
import static gr.ds.unipi.spatialnodb.dataloading.HilbertUtil.doesTrajectoryIntersectWithCube;
import static org.apache.parquet.filter2.predicate.FilterApi.*;

public class Knn3DQueriesDirectories {
    public static void main(String args[]) throws IOException {

        Config config = loadConfig("queries.conf");

        Config dataLoading = config.getConfig("queries");
        final String parquetPath = dataLoading.getString("parquetPath");
        final String queriesFilePath = dataLoading.getString("queriesFilePath");
        final String metricsPath = dataLoading.getString("metricsPath");
        final int k = dataLoading.getInt("k");

        Config metadata = ConfigFactory.parseFile(new File(parquetPath+ File.separator+"space.metadata")).resolve().getConfig("gridHilbert");
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
        int queriedTrajectoriesCounter = 0;

        ParquetInputFormat.setReadSupportClass(jobWholeTrajectory, TrajectorySegmentReadSupport.class);
        ParquetInputFormat.setReadSupportClass(jobTrajectorySegments, TrajectorySegmentPartialReadSupport.class);

        SparkConf sparkConf = new SparkConf().registerKryoClasses(new Class[]{SpatioTemporalPoint.class,SpatioTemporalPoint[].class, SpatialPoint.class,SpatialPoint[].class, Double.class});
        sparkConf.setAppName("Knn Querying in TrajParquet");
        if (!sparkConf.contains("spark.master")) {
            sparkConf.setMaster("local[*]").set("spark.executor.memory","4g");
        }
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        File[] directories = new File(parquetPath+File.separator+"stIndex").listFiles(File::isDirectory);
        Set<String> directoriesSet = new HashSet<>();
        for (File directory : directories) {
            directoriesSet.add(directory.getName());
        }

        List<Long> times = new ArrayList<>();
        List<Integer> pages = new ArrayList<>();

        BufferedWriter bw = new BufferedWriter(new FileWriter(metricsPath+ File.separator+"knn-queries-trajparquet-"+ Paths.get(queriesFilePath).getFileName().toString().replaceFirst("\\.[^.]+$", "")+"-"+Paths.get(parquetPath).getFileName().toString()+".txt"));
        BufferedReader br = new BufferedReader(new FileReader(queriesFilePath));
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

            final double queryMinLongitude = Double.max(minLon,mbrMinLongitude);
            final double queryMinLatitude = Double.max(minLat,mbrMinLatitude);
            final long queryMinTimestamp = minTime;

            final double queryMaxLongitude = Double.min(maxLon-0.0000001,mbrMaxLongitude);
            final double queryMaxLatitude = Double.min(maxLat-0.0000001,mbrMaxLatitude);
            final long queryMaxTimestamp = maxTime-1000;

            FilterPredicate tAxis = and(gtEq(longColumn("minTimestamp"), queryMinTimestamp), ltEq(longColumn("maxTimestamp"), queryMaxTimestamp));

            long[] hilStart = indexUtils.scale(queryMinLongitude, queryMinLatitude, queryMinTimestamp);//HilbertUtil.scaleGeoTemporalPoint(queryMinLongitude, minLon, maxLon,queryMinLatitude, minLat, maxLat, queryMinTimestamp, minTime, maxTime, maxOrdinates);
            long[] hilEnd = indexUtils.scale(queryMaxLongitude, queryMaxLatitude, queryMaxTimestamp);//HilbertUtil.scaleGeoTemporalPoint(queryMaxLongitude, minLon, maxLon, queryMaxLatitude, minLat, maxLat, queryMaxTimestamp, minTime, maxTime, maxOrdinates);
            Ranges ranges = hilbertCurve.query(hilStart, hilEnd, 0);
            final HashSet<Long> initialCubes = new HashSet<>();

            StringBuilder sb = new StringBuilder();
            if(indexUtils instanceof IndexUtils2D){
                for (Range range : ranges.toList()) {
                    for (long r = range.low(); r <= range.high(); r++) {
                        if(directoriesSet.contains(String.valueOf(r))) {
                            long[] cube = hilbertCurve.point(r);
                            double xMin = minLon + (cube[0] * (maxLon-minLon)/(maxOrdinates+ 1L));
                            double yMin = minLat + (cube[1] * (maxLat-minLat)/(maxOrdinates+ 1L));

                            double xMax = minLon + ((cube[0]+1) * (maxLon-minLon)/(maxOrdinates+ 1L));
                            double yMax = minLat + ((cube[1]+1) * (maxLat-minLat)/(maxOrdinates+ 1L));

                            if(doesTrajectoryIntersectWithCube(trajectoryQuery, xMin, yMin, xMax, yMax)){
                                sb.append(parquetPath+File.separator+"stIndex"+File.separator+r+",");
                                initialCubes.add(r);
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

                        if(doesTrajectoryIntersectWithCube(trajectoryQuery, xMin, yMin, xMax, yMax)) {
                            for (long t = hilStart[2]; t <= hilEnd[2]; t++) {
                                long cubeId = hilbertCurve.index(i,j,t);
                                if(directoriesSet.contains(String.valueOf(cubeId))) {
                                    sb.append(parquetPath+File.separator+"stIndex"+File.separator+cubeId+",");
                                }
                            }
                            initialCubes.add(hilbertCurve.index(i,j,0));
                        }
                    }
                }
            }

            HashMap<String, List<TrajectorySegmentWithMetadata>> identifiedTrajSegments = new HashMap<>();
            HashSet<String> flushedTrajectories = new HashSet<>();

            if(sb.length()!=0){
                sb.deleteCharAt(sb.length()-1);
                ParquetInputFormat.setFilterPredicate(jobTrajectorySegments.getConfiguration(), tAxis);
                JavaPairRDD<Void, TrajectorySegmentWithMetadata> pairRDD = (JavaPairRDD<Void, TrajectorySegmentWithMetadata>) jsc.newAPIHadoopFile(sb.toString(), ParquetInputFormat.class, Void.class, TrajectorySegmentWithMetadata.class, jobTrajectorySegments.getConfiguration());
                pairRDD.collect().forEach(seg->{
                    identifiedTrajSegments.compute(seg._2.getTrajectorySegment().getObjectId(), (i, v) -> {
                        if (v == null) {
                            List<TrajectorySegmentWithMetadata> t = new ArrayList<>();
                            t.add(seg._2);
                            return t;
                        } else {
                            v.add(seg._2);
                            v.sort(Comparator.comparingLong(a -> Math.abs(a.getTrajectorySegment().getSegment())));
                            return v;
                        }
                    });
                });
                flush(identifiedTrajSegments, flushedTrajectories);
            }

            BoundedPriorityQueue trajectoryQueue = BoundedPriorityQueue.newBoundedPriorityQueue(k);

            HashSet<Long> visitedCubes = new HashSet<>(initialCubes);
            PriorityQueue<CellScore> queueCells = new PriorityQueue<>(Comparator.comparingDouble(CellScore::getScore));

            HashSet<Long> temp = new HashSet<>();
            if(indexUtils instanceof IndexUtils2D){
                visitedCubes.forEach(c-> {
                    long[] cube = hilbertCurve.point(c);
                    for (long i = Math.max(0, cube[0]-1); i <= Math.min(maxOrdinates, cube[0]+1); i++) {
                        for (long j = Math.max(0, cube[1]-1); j <= Math.min(maxOrdinates, cube[1]+1); j++) {
                            long cId = hilbertCurve.index(i,j);
                            if(!visitedCubes.contains(cId) && !temp.contains(cId)){
                                long[] cubeNeighbor = hilbertCurve.point(cId);
                                temp.add(cId);
                                queueCells.add(CellScore.newCellScore(cId, HilbertUtil.trajectoryMinDist(minLon + (cubeNeighbor[0] * (maxLon-minLon)/(maxOrdinates+ 1L)),minLat + (cubeNeighbor[1] * (maxLat-minLat)/(maxOrdinates+ 1L)),minLon + ((cubeNeighbor[0]+1) * (maxLon-minLon)/(maxOrdinates+ 1L)),minLat + ((cubeNeighbor[1]+1) * (maxLat-minLat)/(maxOrdinates+ 1L)),trajectoryQuery)));
                            }
                        }
                    }
                });
            }else{
                visitedCubes.forEach(c-> {
                    long[] cube = hilbertCurve.point(c);
                    for (long i = Math.max(0, cube[0]-1); i <= Math.min(maxOrdinates, cube[0]+1); i++) {
                        for (long j = Math.max(0, cube[1]-1); j <= Math.min(maxOrdinates, cube[1]+1); j++) {
                            long cId = hilbertCurve.index(i,j,0);
                            if(!visitedCubes.contains(cId) && !temp.contains(cId)){
                                long[] cubeNeighbor = hilbertCurve.point(cId);
                                temp.add(cId);
                                queueCells.add(CellScore.newCellScore(cId, HilbertUtil.trajectoryMinDist(minLon + (cubeNeighbor[0] * (maxLon-minLon)/(maxOrdinates+ 1L)),minLat + (cubeNeighbor[1] * (maxLat-minLat)/(maxOrdinates+ 1L)),minLon + ((cubeNeighbor[0]+1) * (maxLon-minLon)/(maxOrdinates+ 1L)),minLat + ((cubeNeighbor[1]+1) * (maxLat-minLat)/(maxOrdinates+ 1L)),trajectoryQuery)));
                            }
                        }
                    }
                });
            }

            if(flushedTrajectories.size()>=k || queueCells.isEmpty()) {

                Iterator<String> it = flushedTrajectories.iterator();
                FilterPredicate filterPredicate = eq(binaryColumn("objectId"), Binary.fromCharSequence(it.next()));
                while (it.hasNext()) {
                    filterPredicate = or(eq(binaryColumn("objectId"), Binary.fromCharSequence(it.next())), filterPredicate);
                }
                ParquetInputFormat.setFilterPredicate(jobWholeTrajectory.getConfiguration(), filterPredicate);
                JavaPairRDD<Void, TrajectorySegment> pairRDD = (JavaPairRDD<Void, TrajectorySegment>) jsc.newAPIHadoopFile(parquetPath + File.separator + "idIndex", ParquetInputFormat.class, Void.class, TrajectorySegment.class, jobWholeTrajectory.getConfiguration());
                List<TrajectoryScore> ts = pairRDD.map(t -> TrajectoryScore.newTrajectoryScore(t._2, HilbertUtil.frechetDistance(trajectoryQuery, t._2.getSpatioTemporalPoints()))).collect();
                ts.forEach(trajectoryQueue::add);
                queriedTrajectoriesCounter += flushedTrajectories.size();
                flushedTrajectories.clear();
            }

            while((trajectoryQueue.getSize() < k || queueCells.peek().getScore() < trajectoryQueue.getMaxScore()) && !queueCells.isEmpty()) {
                if (trajectoryQueue.getSize() == k){
                    identifiedTrajSegments.entrySet().removeIf(e -> {
                        for (TrajectorySegmentWithMetadata seg : e.getValue()) {

//                            if (HilbertUtil.isMinMaxDistGreaterThan(seg.getTrajectorySegment().getMinLongitude(), seg.getTrajectorySegment().getMinLatitude(), seg.getTrajectorySegment().getMaxLongitude(), seg.getTrajectorySegment().getMaxLatitude(), trajectoryQuery, trajectoryQueue.getMaxScore())) {
//                                return true;
//                            }

                            if (seg.getPivots() != null) {
                                if (seg.getTrajectorySegment().getSegment() > 1) {
                                    for (SpatialPoint pivot : seg.getPivots()) {
//                                        if (HilbertUtil.isPointMinDistGreaterThan(pivot.getLongitude(), pivot.getLatitude(), trajectoryQuery, trajectoryQueue.getMaxScore())) {
                                        if (HilbertUtil.minDistPointToRectangle(pivot.getLongitude(), pivot.getLatitude(), queryMinLongitude, queryMinLatitude, queryMaxLongitude, queryMaxLatitude)> trajectoryQueue.getMaxScore()) {
                                            return true;
                                        }
                                    }
                                }

                                if (seg.getTrajectorySegment().getSegment() == 1) {
                                    if (HilbertUtil.euclideanDistance(seg.getPivots()[seg.getPivots().length - 1].getLongitude(), seg.getPivots()[seg.getPivots().length - 1].getLatitude(), trajectoryQuery[0].getLongitude(), trajectoryQuery[0].getLatitude()) > trajectoryQueue.getMaxScore()) {
                                        return true;
                                    }
                                } else if (seg.getTrajectorySegment().getSegment() < -1) {
                                    if (HilbertUtil.euclideanDistance(seg.getPivots()[seg.getPivots().length - 1].getLongitude(), seg.getPivots()[seg.getPivots().length - 1].getLatitude(), trajectoryQuery[trajectoryQuery.length - 1].getLongitude(), trajectoryQuery[trajectoryQuery.length - 1].getLatitude()) > trajectoryQueue.getMaxScore()) {
                                        return true;
                                    }
                                } else if (seg.getTrajectorySegment().getSegment() == -1) {
                                    if (HilbertUtil.euclideanDistance(seg.getPivots()[seg.getPivots().length - 2].getLongitude(), seg.getPivots()[seg.getPivots().length - 2].getLatitude(), trajectoryQuery[0].getLongitude(), trajectoryQuery[0].getLatitude()) > trajectoryQueue.getMaxScore()) {
                                        return true;
                                    }
                                    if (HilbertUtil.euclideanDistance(seg.getPivots()[seg.getPivots().length - 1].getLongitude(), seg.getPivots()[seg.getPivots().length - 1].getLatitude(), trajectoryQuery[trajectoryQuery.length - 1].getLongitude(), trajectoryQuery[trajectoryQuery.length - 1].getLatitude()) > trajectoryQueue.getMaxScore()) {
                                        return true;
                                    }
                                }
                            }
                        }
                        return false;
                    });
                }

                long cell = queueCells.poll().getCellId();
                visitedCubes.add(cell);

                sb.setLength(0);

                if(indexUtils instanceof IndexUtils2D){
                    long[] cube = hilbertCurve.point(cell);
                    for (long i = Math.max(0, cube[0]-1); i <= Math.min(maxOrdinates, cube[0]+1); i++) {
                        for (long j = Math.max(0, cube[1]-1); j <= Math.min(maxOrdinates, cube[1]+1); j++) {
                            long cId = hilbertCurve.index(i,j);
                            if(!visitedCubes.contains(cId) && !temp.contains(cId)){
                                long[] cubeNeighbor = hilbertCurve.point(cId);
                                temp.add(cId);
                                queueCells.add(CellScore.newCellScore(cId, HilbertUtil.trajectoryMinDist(minLon + (cubeNeighbor[0] * (maxLon-minLon)/(maxOrdinates+ 1L)),minLat + (cubeNeighbor[1] * (maxLat-minLat)/(maxOrdinates+ 1L)),minLon + ((cubeNeighbor[0]+1) * (maxLon-minLon)/(maxOrdinates+ 1L)),minLat + ((cubeNeighbor[1]+1) * (maxLat-minLat)/(maxOrdinates+ 1L)),trajectoryQuery)));
                            }
                        }
                    }
                    String j = String.valueOf(hilbertCurve.index(cube[0], cube[1]));
                    if(directoriesSet.contains(j)) {
                        sb.append(parquetPath+File.separator+"stIndex"+File.separator+j+"/,");
                    }
                }else{
                    long[] cube = hilbertCurve.point(cell);
                    for (long i = Math.max(0, cube[0]-1); i <= Math.min(maxOrdinates, cube[0]+1); i++) {
                        for (long j = Math.max(0, cube[1]-1); j <= Math.min(maxOrdinates, cube[1]+1); j++) {
                            long cId = hilbertCurve.index(i,j,0);
                            if(!visitedCubes.contains(cId) && !temp.contains(cId)){
                                long[] cubeNeighbor = hilbertCurve.point(cId);
                                temp.add(cId);
                                queueCells.add(CellScore.newCellScore(cId, HilbertUtil.trajectoryMinDist(minLon + (cubeNeighbor[0] * (maxLon-minLon)/(maxOrdinates+ 1L)),minLat + (cubeNeighbor[1] * (maxLat-minLat)/(maxOrdinates+ 1L)),minLon + ((cubeNeighbor[0]+1) * (maxLon-minLon)/(maxOrdinates+ 1L)),minLat + ((cubeNeighbor[1]+1) * (maxLat-minLat)/(maxOrdinates+ 1L)),trajectoryQuery)));
                            }
                        }
                    }
                    for (long i = hilStart[2]; i <= hilEnd[2]; i++) {//TIME NEEDED
                        String j = String.valueOf(hilbertCurve.index(cube[0], cube[1], i));
                        if(directoriesSet.contains(j)) {
                            sb.append(parquetPath+File.separator+"stIndex"+File.separator+j+"/,");
                        }
                    }
                }

                if(sb.length()!=0){
                    sb.deleteCharAt(sb.length()-1);

                    JavaPairRDD<Void, TrajectorySegmentWithMetadata> pairRDD = (JavaPairRDD<Void, TrajectorySegmentWithMetadata>) jsc.newAPIHadoopFile(sb.toString(), ParquetInputFormat.class, Void.class, TrajectorySegmentWithMetadata.class, jobTrajectorySegments.getConfiguration());

                    //MBR pruning
                    if(trajectoryQueue.getSize()==k){
                        double l = trajectoryQueue.getMaxScore();
                        pairRDD = pairRDD.filter((tr)->{
                            TrajectorySegmentWithMetadata seg = tr._2;
//                            if (HilbertUtil.isMinMaxDistGreaterThan(seg.getTrajectorySegment().getMinLongitude(), seg.getTrajectorySegment().getMinLatitude(), seg.getTrajectorySegment().getMaxLongitude(), seg.getTrajectorySegment().getMaxLatitude(), trajectoryQuery, l)) {
//                                return false;
//                            }

                            if (seg.getPivots() != null) {
                                if(seg.getTrajectorySegment().getSegment()>1){
                                    for (SpatialPoint pivot : seg.getPivots()) {
                                        if(HilbertUtil.isPointMinDistGreaterThan(pivot.getLongitude(), pivot.getLatitude(), trajectoryQuery, l)){
//                                        if (HilbertUtil.minDistPointToRectangle(pivot.getLongitude(), pivot.getLatitude(), queryMinLongitude, queryMinLatitude, queryMaxLongitude, queryMaxLatitude)> l) {
                                            return false;
                                        }
                                    }
                                }

                                if(seg.getTrajectorySegment().getSegment()==1){
                                    if(HilbertUtil.euclideanDistance(seg.getPivots()[seg.getPivots().length-1].getLongitude(),seg.getPivots()[seg.getPivots().length-1].getLatitude(),trajectoryQuery[0].getLongitude(),trajectoryQuery[0].getLatitude())>l){
                                        return false;
                                    }
                                }else if(seg.getTrajectorySegment().getSegment()<-1){
                                    if(HilbertUtil.euclideanDistance(seg.getPivots()[seg.getPivots().length-1].getLongitude(),seg.getPivots()[seg.getPivots().length-1].getLatitude(),trajectoryQuery[trajectoryQuery.length-1].getLongitude(),trajectoryQuery[trajectoryQuery.length-1].getLatitude())>l){
                                        return false;
                                    }
                                }else if(seg.getTrajectorySegment().getSegment()==-1){
                                    if(HilbertUtil.euclideanDistance(seg.getPivots()[seg.getPivots().length-2].getLongitude(),seg.getPivots()[seg.getPivots().length-2].getLatitude(),trajectoryQuery[0].getLongitude(),trajectoryQuery[0].getLatitude())>l){
                                        return false;
                                    }
                                    if(HilbertUtil.euclideanDistance(seg.getPivots()[seg.getPivots().length-1].getLongitude(),seg.getPivots()[seg.getPivots().length-1].getLatitude(),trajectoryQuery[trajectoryQuery.length-1].getLongitude(),trajectoryQuery[trajectoryQuery.length-1].getLatitude())>l){
                                        return false;
                                    }
                                }
                            }
                            return true;
                        });
                    }

                    pairRDD.collect().forEach(seg->{
                        identifiedTrajSegments.compute(seg._2.getTrajectorySegment().getObjectId(), (i, v) -> {
                            if (v == null) {
                                List<TrajectorySegmentWithMetadata> t = new ArrayList<>();
                                t.add(seg._2);
                                return t;
                            } else {
                                v.add(seg._2);
                                v.sort(Comparator.comparingLong(a -> Math.abs(a.getTrajectorySegment().getSegment())));
                                return v;
                            }
                        });
                    });

                    flush(identifiedTrajSegments, flushedTrajectories);
                }

                if(!flushedTrajectories.isEmpty() && ( flushedTrajectories.size()>= (k-trajectoryQueue.getSize()) || queueCells.isEmpty())){
                    Iterator<String> it = flushedTrajectories.iterator();
                    FilterPredicate filterPredicate = eq(binaryColumn("objectId"), Binary.fromCharSequence(it.next()));
                    while (it.hasNext()) {
                        filterPredicate = or(eq(binaryColumn("objectId"), Binary.fromCharSequence(it.next())), filterPredicate);
                    }

                    ParquetInputFormat.setFilterPredicate(jobWholeTrajectory.getConfiguration(), filterPredicate);
                    JavaPairRDD<Void, TrajectorySegment> pairRDD = (JavaPairRDD<Void, TrajectorySegment>) jsc.newAPIHadoopFile(parquetPath + "/" + "idIndex", ParquetInputFormat.class, Void.class, TrajectorySegment.class, jobWholeTrajectory.getConfiguration());

                    List<TrajectoryScore> ts = pairRDD.map(t -> TrajectoryScore.newTrajectoryScore(t._2, HilbertUtil.frechetDistance(trajectoryQuery, t._2.getSpatioTemporalPoints()))).collect();
                    ts.forEach(trajectoryQueue::add);
                    queriedTrajectoriesCounter += flushedTrajectories.size();

                    flushedTrajectories.clear();
                    if(queueCells.isEmpty()){
                        break;
                    }
                }
            }

//            trajectoryQueue.getMaxHeap().forEach(d->{
//                System.out.println("object id:"+d.getTrajectorySegment().getObjectId()+" score:"+d.getScore());
//            });

            System.out.println("The max score is:"+trajectoryQueue.getMaxScore() +"examined trajectoreies are"+queriedTrajectoriesCounter);



            long num =trajectoryQueue.getSize();// trajs.size();

            long endTime = System.currentTimeMillis();
            times.add((endTime - startTime));
            pages.add(DataPage.counter);

            bw.write((endTime - startTime)+";"+num+";"+trajectoryQuery.length+";"+ DataPage.counter+";"+queriedTrajectoriesCounter);
            DataPage.counter = 0;
            queriedTrajectoriesCounter = 0;
            bw.newLine();

        }
        bw.close();
        br.close();

        for (int ind = 0; ind < 10; ind++) {
            times.remove(0);
            pages.remove(0);
        }
        bw = new BufferedWriter(new FileWriter(metricsPath+ File.separator+"metrics-knn-queries-trajparquet-"+ Paths.get(queriesFilePath).getFileName().toString().replaceFirst("\\.[^.]+$", "")+"-"+Paths.get(parquetPath).getFileName().toString()+".txt"));
        bw.write("Total Time (ms)\tAvg Time (ms)\tTotal Pages\tAvg Pages\n");
        bw.write(times.stream().mapToLong(Long::longValue).sum() + "\t"+ times.stream().mapToLong(Long::longValue).average().getAsDouble() + "\t");
        bw.write(pages.stream().mapToInt(Integer::intValue).sum() + "\t"+ pages.stream().mapToInt(Integer::intValue).average().getAsDouble());
        bw.close();
    }

    private static void flush(HashMap<String, List<TrajectorySegmentWithMetadata>> identifiedTrajectories, HashSet<String> flushedtrajectories){
        identifiedTrajectories.entrySet().removeIf(e->{
            if(e.getValue().get(0).getTrajectorySegment().getSegment()==-1){
                flushedtrajectories.add(e.getValue().get(0).getTrajectorySegment().getObjectId());
                return true;
            }else if(e.getValue().get(0).getTrajectorySegment().getSegment()==1 && e.getValue().get(e.getValue().size()-1).getTrajectorySegment().getSegment()<1){
                for (int i = 0; i < e.getValue().size()-1; i++) {
                    if(e.getValue().get(i).getTrajectorySegment().getSegment()+1!=Math.abs(e.getValue().get(i+1).getTrajectorySegment().getSegment())){
                        return false;
                    }
                }
                flushedtrajectories.add(e.getValue().get(0).getTrajectorySegment().getObjectId());
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
