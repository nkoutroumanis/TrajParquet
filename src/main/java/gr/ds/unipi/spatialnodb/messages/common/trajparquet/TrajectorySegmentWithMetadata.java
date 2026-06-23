package gr.ds.unipi.spatialnodb.messages.common.trajparquet;

import gr.ds.unipi.spatialnodb.messages.common.SpatialPoint;

import java.io.Serializable;

public class TrajectorySegmentWithMetadata implements Serializable {

    private final TrajectorySegment trajectorySegment;
    private final SpatialPoint[] pivots;
    private final long[] interval;

    private TrajectorySegmentWithMetadata(TrajectorySegment trajectorySegment, SpatialPoint[] pivots, long[] interval) {
        this.trajectorySegment = trajectorySegment;
        this.pivots = pivots;
        this.interval = interval;
    }

    public static TrajectorySegmentWithMetadata newTrajectorySegmentWithMetadata(TrajectorySegment trajectorySegment, SpatialPoint[] pivots, long[] interval) {
        return new TrajectorySegmentWithMetadata(trajectorySegment, pivots, interval);
    }

    public TrajectorySegment getTrajectorySegment() {
        return trajectorySegment;
    }

    public SpatialPoint[] getPivots() {
        return pivots;
    }

    public long[] getInterval() {
        return interval;
    }

}