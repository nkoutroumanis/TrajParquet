package gr.ds.unipi.spatialnodb.messages.common.trajparquet;

import gr.ds.unipi.spatialnodb.messages.common.SpatialPoint;

import java.io.Serializable;

public class TrajectorySegmentWithIntervalMetadata implements Serializable {

    private final TrajectorySegment trajectorySegment;
    private final long[] interval;

    private TrajectorySegmentWithIntervalMetadata(TrajectorySegment trajectorySegment, long[] interval) {
        this.trajectorySegment = trajectorySegment;
        this.interval = interval;
    }

    public static TrajectorySegmentWithIntervalMetadata newTrajectorySegmentWithIntervalMetadata(TrajectorySegment trajectorySegment, long[] interval) {
        return new TrajectorySegmentWithIntervalMetadata(trajectorySegment, interval);
    }

    public TrajectorySegment getTrajectorySegment() {
        return trajectorySegment;
    }

    public long[] getInterval() {
        return interval;
    }
}