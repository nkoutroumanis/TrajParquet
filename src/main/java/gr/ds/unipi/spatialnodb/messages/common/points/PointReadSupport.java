package gr.ds.unipi.spatialnodb.messages.common.points;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.util.Map;

public class PointReadSupport extends ReadSupport<Point> {

    @Override
    public ReadContext init(InitContext context){
        return new ReadContext(context.getFileSchema());
    }

    @Override
    public RecordMaterializer<Point> prepareForRead(Configuration configuration, Map<String, String> map, MessageType messageType, ReadContext readContext) {
        return new PointMaterializer();
    }
}
