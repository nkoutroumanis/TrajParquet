package gr.ds.unipi.spatialnodb.parquet;

import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.filter.ColumnPredicates;
import org.apache.parquet.filter.ColumnRecordFilter;
import org.apache.parquet.filter.RecordFilter;
import org.apache.parquet.filter.UnboundRecordFilter;
import org.apache.parquet.filter2.predicate.FilterPredicate;

public class Fil implements UnboundRecordFilter {
    @Override
    public RecordFilter bind(Iterable<ColumnReader> iterable) {
        return ColumnRecordFilter.column("id", ColumnPredicates.applyFunctionToInteger(new ColumnPredicates.IntegerPredicateFunction() {
            @Override
            public boolean functionToApply(int i) {
                return i<100;
            }
        })).bind(iterable);
    }
}
