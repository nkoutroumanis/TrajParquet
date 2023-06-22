package gr.ds.unipi.spatialnodb.messages.common.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.util.Map;

public class LineReadSupport extends ReadSupport<Line> {

    @Override
    public ReadContext init(InitContext context){
        return new ReadContext(context.getFileSchema());
    }

    @Override
    public RecordMaterializer<Line> prepareForRead(Configuration configuration, Map<String, String> map, MessageType messageType, ReadContext readContext) {
        return new LineMaterializer();
    }
}
