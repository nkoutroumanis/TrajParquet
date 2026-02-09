package gr.ds.unipi.spatialnodb.messages.common.trajparquet;

public class TrajectoryScore {
    private final TrajectorySegment trajectorySegment;
    private final double score;
    private TrajectoryScore(TrajectorySegment trajectorySegment, double score) {
        this.trajectorySegment = trajectorySegment;
        this.score = score;
    }
    public TrajectorySegment getTrajectorySegment() {
        return trajectorySegment;
    }
    public double getScore() {
        return score;
    }
    public static TrajectoryScore newTrajectoryScore(TrajectorySegment trajectorySegment, double score) {
        return new TrajectoryScore(trajectorySegment, score);
    }
}
