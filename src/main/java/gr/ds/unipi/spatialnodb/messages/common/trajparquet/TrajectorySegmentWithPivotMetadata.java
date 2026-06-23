package gr.ds.unipi.spatialnodb.messages.common.trajparquet;

import gr.ds.unipi.spatialnodb.messages.common.SpatialPoint;

import java.io.Serializable;

public class TrajectorySegmentWithPivotMetadata implements Serializable {

    private final TrajectorySegment trajectorySegment;
    private final SpatialPoint[] pivots;

    private TrajectorySegmentWithPivotMetadata(TrajectorySegment trajectorySegment, SpatialPoint[] pivots) {
        this.trajectorySegment = trajectorySegment;
        this.pivots = pivots;
    }

    public static TrajectorySegmentWithPivotMetadata newTrajectorySegmentWithPivotMetadata(TrajectorySegment trajectorySegment, SpatialPoint[] pivots) {
        return new TrajectorySegmentWithPivotMetadata(trajectorySegment, pivots);
    }

    public TrajectorySegment getTrajectorySegment() {
        return trajectorySegment;
    }

    public SpatialPoint[] getPivots() {
        return pivots;
    }
}