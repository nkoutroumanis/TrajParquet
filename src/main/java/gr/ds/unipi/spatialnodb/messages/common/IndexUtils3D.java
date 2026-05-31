package gr.ds.unipi.spatialnodb.messages.common;

import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.shapes.STPoint;
import org.davidmoten.hilbert.Ranges;

import java.util.Optional;
import java.util.Set;

public class IndexUtils3D extends IndexUtils {

    private final long minTime;
    private final long maxTime;

    public IndexUtils3D(double minLon, double minLat, long minTime, double maxLon, double maxLat, long maxTime, long maxOrdinates) {
        super(minLon, minLat, maxLon, maxLat, maxOrdinates);
        this.minTime = minTime;
        this.maxTime = maxTime;
    }

    @Override
    public long[] scale(double lon, double lat, long time) {
        long x = scale((lon - minLon) / (maxLon - minLon), maxOrdinates);
        long y = scale((lat - minLat) / (maxLat - minLat), maxOrdinates);
        long z = scale(((double) time - minTime) / (maxTime - minTime), maxOrdinates);
        return new long[]{x, y, z};
    }

//    @Override
//    public Optional<STPoint[]> clipping(long[] cube, SpatioTemporalPoint spt1, SpatioTemporalPoint spt2) {
//        double xMin = minLon + (cube[0] * (maxLon-minLon)/(maxOrdinates+ 1L));
//        double yMin = minLat + (cube[1] * (maxLat-minLat)/(maxOrdinates+ 1L));
//
//        double xMax = minLon + ((cube[0]+1) * (maxLon-minLon)/(maxOrdinates+ 1L));
//        double yMax = minLat + ((cube[1]+1) * (maxLat-minLat)/(maxOrdinates+ 1L));
//
//        long tMin = minTime + (cube[2] * (maxTime-minTime)/(maxOrdinates+ 1L));
//        long tMax = minTime + ((cube[2]+1) * (maxTime-minTime)/(maxOrdinates+ 1L));
//        return HilbertUtil.liangBarsky(spt1.getLongitude(), spt1.getLatitude(), spt1.getTimestamp(), spt2.getLongitude(), spt2.getLatitude(), spt2.getTimestamp(), xMin, yMin, tMin, xMax, yMax, tMax);
//    }

    @Override
    public Optional<STPoint[]> clipping(long[] cube, double x1, double y1, long t1, double x2, double y2, long t2) {
        double xMin = minLon + (cube[0] * (maxLon-minLon)/(maxOrdinates+ 1L));
        double yMin = minLat + (cube[1] * (maxLat-minLat)/(maxOrdinates+ 1L));

        double xMax = minLon + ((cube[0]+1) * (maxLon-minLon)/(maxOrdinates+ 1L));
        double yMax = minLat + ((cube[1]+1) * (maxLat-minLat)/(maxOrdinates+ 1L));

        long tMin = minTime + (cube[2] * (maxTime-minTime)/(maxOrdinates+ 1L));
        long tMax = minTime + ((cube[2]+1) * (maxTime-minTime)/(maxOrdinates+ 1L));
        return HilbertUtil.liangBarsky(x1, y1, t1, x2, y2, t2, xMin, yMin, tMin, xMax, yMax, tMax);

    }

    @Override
    public void setFiltersAndCells2DQuery(long[] hilStart, long[] hilEnd, Ranges ranges, Set<String> directoriesSet, StringBuilder sbIntersected, StringBuilder sbFullyCovers) {

    }

    @Override
    public void setFiltersAndCells3DQuery(long[] hilStart, long[] hilEnd, Ranges ranges, Set<String> directoriesSet, StringBuilder sbIntersected, StringBuilder sbFullyCovers) {

    }
}
