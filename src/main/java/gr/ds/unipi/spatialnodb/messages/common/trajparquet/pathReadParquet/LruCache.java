package gr.ds.unipi.spatialnodb.messages.common.trajparquet.pathReadParquet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

public class LruCache<K, V extends LruCache.Value<K, V>> {
    private static final Logger LOG = LoggerFactory.getLogger(LruCache.class);
    private static final float DEFAULT_LOAD_FACTOR = 0.75F;
    private final LinkedHashMap<K, V> cacheMap;

    public LruCache(int maxSize) {
        this(maxSize, 0.75F, true);
    }

    public LruCache(final int maxSize, float loadFactor, boolean accessOrder) {
        int initialCapacity = Math.round((float)maxSize / loadFactor);
        this.cacheMap = new LinkedHashMap<K, V>(initialCapacity, loadFactor, accessOrder) {
            public boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                boolean result = this.size() > maxSize;
                if (result && LOG.isDebugEnabled()) {
                    LOG.debug("Removing eldest entry in cache: " + eldest.getKey());
                }

                return result;
            }
        };
    }

    public V remove(K key) {
        V oldValue = this.cacheMap.remove(key);
        if (oldValue != null) {
            LOG.debug("Removed cache entry for '{}'", key);
        }

        return oldValue;
    }

    public void put(K key, V newValue) {
        if (newValue != null && newValue.isCurrent(key)) {
            V oldValue = this.cacheMap.get(key);
            if (oldValue != null && oldValue.isNewerThan(newValue)) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("Ignoring new cache entry for '{}' because existing cache entry is newer", key);
                }

            } else {
                oldValue = this.cacheMap.put(key, newValue);
                if (LOG.isDebugEnabled()) {
                    if (oldValue == null) {
                        LOG.debug("Added new cache entry for '{}'", key);
                    } else {
                        LOG.debug("Overwrote existing cache entry for '{}'", key);
                    }
                }

            }
        } else {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Ignoring new cache entry for '{}' because it is {}", key, newValue == null ? "null" : "not current");
            }

        }
    }

    public void clear() {
        this.cacheMap.clear();
    }

    public V getCurrentValue(K key) {
        V value = this.cacheMap.get(key);
        LOG.debug("Value for '{}' {} in cache", key, value == null ? "not " : "");
        if (value != null && !value.isCurrent(key)) {
            this.remove(key);
            return null;
        } else {
            return value;
        }
    }

    public int size() {
        return this.cacheMap.size();
    }

    interface Value<K, V> {
        boolean isCurrent(K var1);

        boolean isNewerThan(V var1);
    }
}
