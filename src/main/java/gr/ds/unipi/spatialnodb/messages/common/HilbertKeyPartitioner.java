package gr.ds.unipi.spatialnodb.messages.common;

import org.apache.spark.Partitioner;

public class HilbertKeyPartitioner extends Partitioner {
    private final int n;

    public HilbertKeyPartitioner(int n) {
        this.n = n;
    }

    @Override
    public int numPartitions() {
        return n;
    }

    @Override
    public int getPartition(Object key) {
        HilbertKeyTimestamp k = (HilbertKeyTimestamp) key;
        return (int) (Math.abs(k.getHilbertKey()) % n);
    }
}
