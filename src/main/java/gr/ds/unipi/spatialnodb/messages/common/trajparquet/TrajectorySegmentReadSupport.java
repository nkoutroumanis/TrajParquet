package gr.ds.unipi.spatialnodb.messages.common.trajparquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.util.Map;

public class TrajectorySegmentReadSupport extends ReadSupport<TrajectorySegment> {

    @Override
    public ReadContext init(InitContext context){

        MessageType schema = MessageTypeParser.parseMessageType( "message TrajectorySegment {\n" +
                "required BINARY objectId;\n" +
                "required INT64 segment;\n" +
                "required BINARY longitude;\n" +
                "required BINARY latitude;\n" +
                "required BINARY timestamps;\n" +
                "required DOUBLE minLongitude;\n" +
                "required DOUBLE minLatitude;\n" +
                "required INT64 minTimestamp;\n" +
                "required DOUBLE maxLongitude;\n" +
                "required DOUBLE maxLatitude;\n" +
                "required INT64 maxTimestamp;\n" +
                "}");

        return new ReadContext(schema/*context.getFileSchema()*/);
    }

    @Override
    public RecordMaterializer<TrajectorySegment> prepareForRead(Configuration configuration, Map<String, String> map, MessageType messageType, ReadContext readContext) {
        return new TrajectorySegmentMaterializer();
    }
}
