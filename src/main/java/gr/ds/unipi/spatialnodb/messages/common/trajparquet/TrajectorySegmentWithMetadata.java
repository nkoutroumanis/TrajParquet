package gr.ds.unipi.spatialnodb.messages.common.trajparquet;

import java.io.Serializable;

public class TrajectorySegmentWithMetadata implements Serializable {

    private final TrajectorySegment trajectorySegment;
    private final SpatialPoint[] pivots;

    private TrajectorySegmentWithMetadata(TrajectorySegment trajectorySegment, SpatialPoint[] pivots) {
        this.trajectorySegment = trajectorySegment;
        this.pivots = pivots;
    }

    public static TrajectorySegmentWithMetadata newTrajectorySegmentWithMetadata(TrajectorySegment trajectorySegment, SpatialPoint[] pivots) {
        return new TrajectorySegmentWithMetadata(trajectorySegment, pivots);
    }

    public TrajectorySegment getTrajectorySegment() {
        return trajectorySegment;
    }

    public SpatialPoint[] getPivots() {
        return pivots;
    }
}