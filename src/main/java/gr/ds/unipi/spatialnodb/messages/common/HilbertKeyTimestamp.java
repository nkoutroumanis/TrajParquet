package gr.ds.unipi.spatialnodb.messages.common;

import java.io.Serializable;
import java.util.Objects;

public class HilbertKeyTimestamp implements Comparable<HilbertKeyTimestamp>, Serializable {
    public long getHilbertKey() {
        return hilbertKey;
    }

    public long getTimestamp() {
        return timestamp;
    }

    private final long hilbertKey;
        private final long timestamp;

        public HilbertKeyTimestamp(long hilbertKey, long timestamp) {
            this.hilbertKey = hilbertKey;
            this.timestamp = timestamp;
        }

        @Override
        public int compareTo(HilbertKeyTimestamp o) {
            int c = Long.compare(this.hilbertKey, o.hilbertKey);
            if (c != 0) return c;
            return Long.compare(this.timestamp, o.timestamp);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof HilbertKeyTimestamp)) return false;
            HilbertKeyTimestamp other = (HilbertKeyTimestamp) o;
            return this.hilbertKey == other.hilbertKey &&
                    this.timestamp == other.timestamp;
        }

    @Override
    public int hashCode() {
        return Objects.hash(hilbertKey, timestamp);
    }
}
