package gr.ds.unipi.spatialnodb.messages.common;

import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.shapes.STPoint;
import org.davidmoten.hilbert.Ranges;

import java.util.Optional;
import java.util.Set;

public class IndexUtils2D extends IndexUtils {

    public IndexUtils2D(double minLon, double minLat, double maxLon, double maxLat, long maxOrdinates) {
        super(minLon, minLat, maxLon, maxLat, maxOrdinates);
    }

    @Override
    public long[] scale(double lon, double lat, long time) {
        long x = scale((lon - minLon) / (maxLon - minLon), maxOrdinates);
        long y = scale((lat - minLat) / (maxLat - minLat), maxOrdinates);
        return new long[]{x, y};
    }

//    @Override
//    public Optional<STPoint[]> clipping(long[] cube, SpatioTemporalPoint spt1, SpatioTemporalPoint spt2) {
//        double xMin = minLon + (cube[0] * (maxLon-minLon)/(maxOrdinates+ 1L));
//        double yMin = minLat + (cube[1] * (maxLat-minLat)/(maxOrdinates+ 1L));
//
//        double xMax = minLon + ((cube[0]+1) * (maxLon-minLon)/(maxOrdinates+ 1L));
//        double yMax = minLat + ((cube[1]+1) * (maxLat-minLat)/(maxOrdinates+ 1L));
//
//        return HilbertUtil.liangBarskyTimeInterpolation(spt1.getLongitude(), spt1.getLatitude(), spt1.getTimestamp(), spt2.getLongitude(), spt2.getLatitude(), spt2.getTimestamp(), xMin, yMin, xMax, yMax);
//    }

    @Override
    public Optional<STPoint[]> clipping(long[] cube, double x1, double y1, long t1, double x2, double y2, long t2) {

        double xMin = minLon + (cube[0] * (maxLon-minLon)/(maxOrdinates+ 1L));
        double yMin = minLat + (cube[1] * (maxLat-minLat)/(maxOrdinates+ 1L));

        double xMax = minLon + ((cube[0]+1) * (maxLon-minLon)/(maxOrdinates+ 1L));
        double yMax = minLat + ((cube[1]+1) * (maxLat-minLat)/(maxOrdinates+ 1L));

        return HilbertUtil.liangBarskyTimeInterpolation(x1, y1, t1, x2, y2, t2, xMin, yMin, xMax, yMax);
    }

    @Override
    public void setFiltersAndCells2DQuery(long[] hilStart, long[] hilEnd, Ranges ranges, Set<String> directoriesSet, StringBuilder sbIntersected, StringBuilder sbFullyCovers) {

    }

    @Override
    public void setFiltersAndCells3DQuery(long[] hilStart, long[] hilEnd, Ranges ranges, Set<String> directoriesSet, StringBuilder sbIntersected, StringBuilder sbFullyCovers) {

    }
}
