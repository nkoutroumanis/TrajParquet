package gr.ds.unipi.spatialnodb.messages.common;

import gr.ds.unipi.spatialnodb.shapes.STPoint;
import org.davidmoten.hilbert.Ranges;

import java.io.Serializable;
import java.util.Optional;
import java.util.Set;

public abstract class IndexUtils implements Serializable {
    protected final double minLon;
    protected final double maxLon;
    protected final double minLat;
    protected final double maxLat;
    protected final long maxOrdinates;

    public IndexUtils(double minLon, double minLat, double maxLon, double maxLat, long maxOrdinates) {
        this.minLon = minLon;
        this.maxLon = maxLon;
        this.minLat = minLat;
        this.maxLat = maxLat;
        this.maxOrdinates = maxOrdinates;
    }

    public long[] scale(SpatioTemporalPoint stp){
        return scale(stp.getLongitude(), stp.getLatitude(), stp.getTimestamp());
    }

    public abstract long[] scale(double lon, double lat, long time);

    protected static long scale(double d, long max) {
        if (!(Double.compare(d, 0) != -1 && Double.compare(d, 1) != 1)) {
            throw new IllegalArgumentException();
        }

        if (d == 1) {
            return max;
        } else {
            return Math.round(Math.floor(d * (max + 1)));
        }
    }

    public Optional<STPoint[]> clipping(long[] cube, SpatioTemporalPoint stp1, SpatioTemporalPoint stp2){
        return clipping(cube, stp1.getLongitude(), stp1.getLatitude(), stp1.getTimestamp(), stp2.getLongitude(), stp2.getLatitude(), stp2.getTimestamp());
    }

    public abstract Optional<STPoint[]> clipping(long[] cube, double x1, double y1, long t1, double x2, double y2, long t2);

    public abstract void setFiltersAndCells2DQuery(long[] hilStart, long[] hilEnd, Ranges ranges, Set<String> directoriesSet, StringBuilder sbIntersected, StringBuilder sbFullyCovers);

    public abstract void setFiltersAndCells3DQuery(long[] hilStart, long[] hilEnd, Ranges ranges, Set<String> directoriesSet, StringBuilder sbIntersected, StringBuilder sbFullyCovers);

}
