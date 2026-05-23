package gr.ds.unipi.spatialnodb.messages.common.trajparquet.pathReadParquet;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.parquet.Preconditions;
import org.apache.parquet.filter.UnboundRecordFilter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.*;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.util.ConfigurationUtil;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.hadoop.util.SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class ParquetInputFormatWithKey<T> extends FileInputFormat<Long, T> {
    private static final Logger LOG = LoggerFactory.getLogger(ParquetInputFormatWithKey.class);
    public static final String READ_SUPPORT_CLASS = "parquet.read.support.class";
    public static final String UNBOUND_RECORD_FILTER = "parquet.read.filter";
    public static final String STRICT_TYPE_CHECKING = "parquet.strict.typing";
    public static final String FILTER_PREDICATE = "parquet.private.read.filter.predicate";
    public static final String RECORD_FILTERING_ENABLED = "parquet.filter.record-level.enabled";
    public static final String STATS_FILTERING_ENABLED = "parquet.filter.stats.enabled";
    public static final String DICTIONARY_FILTERING_ENABLED = "parquet.filter.dictionary.enabled";
    public static final String COLUMN_INDEX_FILTERING_ENABLED = "parquet.filter.columnindex.enabled";
    public static final String PAGE_VERIFY_CHECKSUM_ENABLED = "parquet.page.verify-checksum.enabled";
    public static final String BLOOM_FILTERING_ENABLED = "parquet.filter.bloom.enabled";
    public static final String TASK_SIDE_METADATA = "parquet.task.side.metadata";
    public static final String SPLIT_FILES = "parquet.split.files";
    private static final int MIN_FOOTER_CACHE_SIZE = 100;
    private LruCache<ParquetInputFormatWithKey.FileStatusWrapper, ParquetInputFormatWithKey.FootersCacheValue> footersCache;
    private final Class<? extends ReadSupport<T>> readSupportClass;

    public static void setTaskSideMetaData(Job job, boolean taskSideMetadata) {
        ContextUtil.getConfiguration(job).setBoolean("parquet.task.side.metadata", taskSideMetadata);
    }

    public static boolean isTaskSideMetaData(Configuration configuration) {
        return configuration.getBoolean("parquet.task.side.metadata", Boolean.TRUE);
    }

    public static void setReadSupportClass(Job job, Class<?> readSupportClass) {
        ContextUtil.getConfiguration(job).set("parquet.read.support.class", readSupportClass.getName());
    }

    public static void setUnboundRecordFilter(Job job, Class<? extends UnboundRecordFilter> filterClass) {
        Configuration conf = ContextUtil.getConfiguration(job);
        Preconditions.checkArgument(getFilterPredicate(conf) == null, "You cannot provide an UnboundRecordFilter after providing a FilterPredicate");
        conf.set("parquet.read.filter", filterClass.getName());
    }

    /** @deprecated */
    @Deprecated
    public static Class<?> getUnboundRecordFilter(Configuration configuration) {
        return ConfigurationUtil.getClassFromConfig(configuration, "parquet.read.filter", UnboundRecordFilter.class);
    }

    private static UnboundRecordFilter getUnboundRecordFilterInstance(Configuration configuration) {
        Class<?> clazz = ConfigurationUtil.getClassFromConfig(configuration, "parquet.read.filter", UnboundRecordFilter.class);
        if (clazz == null) {
            return null;
        } else {
            try {
                UnboundRecordFilter unboundRecordFilter = (UnboundRecordFilter)clazz.newInstance();
                if (unboundRecordFilter instanceof Configurable) {
                    ((Configurable)unboundRecordFilter).setConf(configuration);
                }

                return unboundRecordFilter;
            } catch (IllegalAccessException | InstantiationException var3) {
                ReflectiveOperationException e = var3;
                throw new BadConfigurationException("could not instantiate unbound record filter class", e);
            }
        }
    }

    public static void setReadSupportClass(JobConf conf, Class<?> readSupportClass) {
        conf.set("parquet.read.support.class", readSupportClass.getName());
    }

    public static Class<?> getReadSupportClass(Configuration configuration) {
        return ConfigurationUtil.getClassFromConfig(configuration, "parquet.read.support.class", ReadSupport.class);
    }

    public static void setFilterPredicate(Configuration configuration, FilterPredicate filterPredicate) {
        Preconditions.checkArgument(getUnboundRecordFilter(configuration) == null, "You cannot provide a FilterPredicate after providing an UnboundRecordFilter");
        configuration.set("parquet.private.read.filter.predicate.human.readable", filterPredicate.toString());

        try {
            SerializationUtil.writeObjectToConfAsBase64("parquet.private.read.filter.predicate", filterPredicate, configuration);
        } catch (IOException var3) {
            IOException e = var3;
            throw new RuntimeException(e);
        }
    }

    private static FilterPredicate getFilterPredicate(Configuration configuration) {
        try {
            return (FilterPredicate)SerializationUtil.readObjectFromConfAsBase64("parquet.private.read.filter.predicate", configuration);
        } catch (IOException var2) {
            IOException e = var2;
            throw new RuntimeException(e);
        }
    }

    public static FilterCompat.Filter getFilter(Configuration conf) {
        return FilterCompat.get(getFilterPredicate(conf), getUnboundRecordFilterInstance(conf));
    }

    public ParquetInputFormatWithKey() {
        this.readSupportClass = null;
    }

    public <S extends ReadSupport<T>> ParquetInputFormatWithKey(Class<S> readSupportClass) {
        this.readSupportClass = readSupportClass;
    }

    public RecordReader<Long, T> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Configuration conf = ContextUtil.getConfiguration(taskAttemptContext);
        ReadSupport<T> readSupport = this.getReadSupport(conf);
        return new ParquetRecordReaderWithKey(readSupport, getFilter(conf));
    }

    /** @deprecated */
    @Deprecated
    ReadSupport<T> getReadSupport(Configuration configuration) {
        if (readSupportClass == null) {
            return getReadSupportInstance((Class)getReadSupportClass(configuration));
        }
        return getReadSupportInstance(this.readSupportClass);

//        return getReadSupportInstance(this.readSupportClass == null ? getReadSupportClass(configuration) : this.readSupportClass);
    }

//    public static <T> ReadSupport<T> getReadSupportInstance(Configuration configuration) {
//        return getReadSupportInstance(getReadSupportClass(configuration));
//    }

    static <T> ReadSupport<T> getReadSupportInstance(Class<? extends ReadSupport<T>> readSupportClass) {
        try {
            return readSupportClass.newInstance();
        } catch (IllegalAccessException | InstantiationException var2) {
            ReflectiveOperationException e = var2;
            throw new BadConfigurationException("could not instantiate read support class", e);
        }
    }

    protected boolean isSplitable(JobContext context, Path filename) {
        return ContextUtil.getConfiguration(context).getBoolean("parquet.split.files", true);
    }

//    public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
//        Configuration configuration = ContextUtil.getConfiguration(jobContext);
//        List<InputSplit> splits = new ArrayList();
//        if (!isTaskSideMetaData(configuration)) {
//            splits.addAll(this.getSplits(configuration, this.getFooters(jobContext)));
//            return splits;
//        } else {
//            Iterator var4 = super.getSplits(jobContext).iterator();
//
//            while(var4.hasNext()) {
//                InputSplit split = (InputSplit)var4.next();
//                Preconditions.checkArgument(split instanceof FileSplit, "Cannot wrap non-FileSplit: " + split);
//                splits.add(ParquetInputSplit.from((FileSplit)split));
//            }
//
//            return splits;
//        }
//    }

//    /** @deprecated */
//    @Deprecated
//    public List<ParquetInputSplit> getSplits(Configuration configuration, List<Footer> footers) throws IOException {
//        boolean strictTypeChecking = configuration.getBoolean("parquet.strict.typing", true);
//        long maxSplitSize = configuration.getLong("mapred.max.split.size", Long.MAX_VALUE);
//        long minSplitSize = Math.max(this.getFormatMinSplitSize(), configuration.getLong("mapred.min.split.size", 0L));
//        if (maxSplitSize >= 0L && minSplitSize >= 0L) {
//            GlobalMetaData globalMetaData = ParquetFileWriter.getGlobalMetaData(footers, strictTypeChecking);
//            ReadSupport.ReadContext readContext = this.getReadSupport(configuration).init(new InitContext(configuration, globalMetaData.getKeyValueMetaData(), globalMetaData.getSchema()));
//            return (new ClientSideMetadataSplitStrategy()).getSplits(configuration, footers, maxSplitSize, minSplitSize, readContext);
//        } else {
//            throw new ParquetDecodingException("maxSplitSize or minSplitSize should not be negative: maxSplitSize = " + maxSplitSize + "; minSplitSize = " + minSplitSize);
//        }
//    }

    protected List<FileStatus> listStatus(JobContext jobContext) throws IOException {
        return getAllFileRecursively(super.listStatus(jobContext), ContextUtil.getConfiguration(jobContext));
    }

    private static List<FileStatus> getAllFileRecursively(List<FileStatus> files, Configuration conf) throws IOException {
        List<FileStatus> result = new ArrayList();
        Iterator var3 = files.iterator();

        while(var3.hasNext()) {
            FileStatus file = (FileStatus)var3.next();
            if (file.isDir()) {
                Path p = file.getPath();
                FileSystem fs = p.getFileSystem(conf);
                staticAddInputPathRecursively(result, fs, p, HiddenFileFilter.INSTANCE);
            } else {
                result.add(file);
            }
        }

        LOG.info("Total input paths to process : {}", result.size());
        return result;
    }

    private static void staticAddInputPathRecursively(List<FileStatus> result, FileSystem fs, Path path, PathFilter inputFilter) throws IOException {
        FileStatus[] var4 = fs.listStatus(path, inputFilter);
        int var5 = var4.length;

        for(int var6 = 0; var6 < var5; ++var6) {
            FileStatus stat = var4[var6];
            if (stat.isDir()) {
                staticAddInputPathRecursively(result, fs, stat.getPath(), inputFilter);
            } else {
                result.add(stat);
            }
        }

    }

    public List<Footer> getFooters(JobContext jobContext) throws IOException {
        List<FileStatus> statuses = this.listStatus(jobContext);
        if (statuses.isEmpty()) {
            return Collections.emptyList();
        } else {
            Configuration config = ContextUtil.getConfiguration(jobContext);
            Map<ParquetInputFormatWithKey.FileStatusWrapper, Footer> footersMap = new LinkedHashMap();
            Set<FileStatus> missingStatuses = new HashSet();
            Map<Path, ParquetInputFormatWithKey.FileStatusWrapper> missingStatusesMap = new HashMap(missingStatuses.size());
            if (this.footersCache == null) {
                this.footersCache = new LruCache(Math.max(statuses.size(), 100));
            }

            Iterator var7 = statuses.iterator();

            while(var7.hasNext()) {
                FileStatus status = (FileStatus)var7.next();
                ParquetInputFormatWithKey.FileStatusWrapper statusWrapper = new ParquetInputFormatWithKey.FileStatusWrapper(status);
                ParquetInputFormatWithKey.FootersCacheValue cacheEntry = (ParquetInputFormatWithKey.FootersCacheValue)this.footersCache.getCurrentValue(statusWrapper);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Cache entry " + (cacheEntry == null ? "not " : "") + " found for '" + status.getPath() + "'");
                }

                if (cacheEntry != null) {
                    footersMap.put(statusWrapper, cacheEntry.getFooter());
                } else {
                    footersMap.put(statusWrapper, null);
                    missingStatuses.add(status);
                    missingStatusesMap.put(status.getPath(), statusWrapper);
                }
            }

            LOG.debug("found {} footers in cache and adding up to {} missing footers to the cache", footersMap.size(), missingStatuses.size());
            Iterator var13;
            if (!missingStatuses.isEmpty()) {
                List<Footer> newFooters = this.getFooters(config, (Collection)missingStatuses);
                var13 = newFooters.iterator();

                while(var13.hasNext()) {
                    Footer newFooter = (Footer)var13.next();
                    ParquetInputFormatWithKey.FileStatusWrapper fileStatus = (ParquetInputFormatWithKey.FileStatusWrapper)missingStatusesMap.get(newFooter.getFile());
                    this.footersCache.put(fileStatus, new ParquetInputFormatWithKey.FootersCacheValue(fileStatus, newFooter));
                }
            }

            List<Footer> footers = new ArrayList(statuses.size());
            var13 = footersMap.entrySet().iterator();

            while(var13.hasNext()) {
                Map.Entry<ParquetInputFormatWithKey.FileStatusWrapper, Footer> footerEntry = (Map.Entry)var13.next();
                Footer footer = (Footer)footerEntry.getValue();
                if (footer == null) {
                    footers.add(((ParquetInputFormatWithKey.FootersCacheValue)this.footersCache.getCurrentValue((ParquetInputFormatWithKey.FileStatusWrapper)footerEntry.getKey())).getFooter());
                } else {
                    footers.add(footer);
                }
            }

            return footers;
        }
    }

    public List<Footer> getFooters(Configuration configuration, List<FileStatus> statuses) throws IOException {
        return this.getFooters(configuration, (Collection)statuses);
    }

    public List<Footer> getFooters(Configuration configuration, Collection<FileStatus> statuses) throws IOException {
        LOG.debug("reading {} files", statuses.size());
        boolean taskSideMetaData = isTaskSideMetaData(configuration);
        return ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(configuration, statuses, taskSideMetaData);
    }

    static final class FileStatusWrapper {
        private final FileStatus status;

        public FileStatusWrapper(FileStatus fileStatus) {
            if (fileStatus == null) {
                throw new IllegalArgumentException("FileStatus object cannot be null");
            } else {
                this.status = fileStatus;
            }
        }

        public long getModificationTime() {
            return this.status.getModificationTime();
        }

        public int hashCode() {
            return this.status.hashCode();
        }

        public boolean equals(Object other) {
            return other instanceof ParquetInputFormatWithKey.FileStatusWrapper && this.status.equals(((ParquetInputFormatWithKey.FileStatusWrapper)other).status);
        }

        public String toString() {
            return this.status.getPath().toString();
        }
    }

    static final class FootersCacheValue implements LruCache.Value<ParquetInputFormatWithKey.FileStatusWrapper, ParquetInputFormatWithKey.FootersCacheValue> {
        private final long modificationTime;
        private final Footer footer;

        public FootersCacheValue(ParquetInputFormatWithKey.FileStatusWrapper status, Footer footer) {
            this.modificationTime = status.getModificationTime();
            this.footer = new Footer(footer.getFile(), footer.getParquetMetadata());
        }

        public boolean isCurrent(ParquetInputFormatWithKey.FileStatusWrapper key) {
            long currentModTime = key.getModificationTime();
            boolean isCurrent = this.modificationTime >= currentModTime;
            if (ParquetInputFormatWithKey.LOG.isDebugEnabled() && !isCurrent) {
                ParquetInputFormatWithKey.LOG.debug("The cache value for '{}' is not current: cached modification time={}, current modification time: {}", new Object[]{key, this.modificationTime, currentModTime});
            }

            return isCurrent;
        }

        public Footer getFooter() {
            return this.footer;
        }

        public boolean isNewerThan(ParquetInputFormatWithKey.FootersCacheValue otherValue) {
            return otherValue == null || this.modificationTime > otherValue.modificationTime;
        }

        public Path getPath() {
            return this.footer.getFile();
        }
    }
}
