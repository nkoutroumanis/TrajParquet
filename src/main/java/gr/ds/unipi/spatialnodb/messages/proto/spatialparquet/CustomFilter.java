package gr.ds.unipi.spatialnodb.messages.proto.spatialparquet;

import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.filter.RecordFilter;
import org.apache.parquet.filter.UnboundRecordFilter;

public class CustomFilter implements UnboundRecordFilter {

    @Override
    public RecordFilter bind(Iterable<ColumnReader> iterable) {
        return null;
    }
}
