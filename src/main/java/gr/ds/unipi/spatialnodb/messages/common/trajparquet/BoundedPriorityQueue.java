package gr.ds.unipi.spatialnodb.messages.common.trajparquet;

import java.util.Comparator;
import java.util.PriorityQueue;

public class BoundedPriorityQueue {
    private final int maxSize;
    private final PriorityQueue<TrajectoryScore> maxHeap;

    private BoundedPriorityQueue(int maxSize) {
        this.maxSize = maxSize;
        this.maxHeap = new PriorityQueue<>(maxSize, Comparator.comparingDouble(TrajectoryScore::getScore).reversed());
    }
    public static BoundedPriorityQueue newBoundedPriorityQueue(int maxSize) {
        return new BoundedPriorityQueue(maxSize);
    }

    public void add(TrajectoryScore ts) {
        if (maxHeap.size() < maxSize) {
            maxHeap.add(ts);
        } else {
            TrajectoryScore worst = maxHeap.peek(); // O(1), always the highest score
            if (ts.getScore() < worst.getScore()) {
                maxHeap.poll();
                maxHeap.add(ts);
            }
        }
    }

    public double getMaxScore() {
        return maxHeap.peek().getScore();
    }

    public int getSize() {
        return maxHeap.size();
    }
}
