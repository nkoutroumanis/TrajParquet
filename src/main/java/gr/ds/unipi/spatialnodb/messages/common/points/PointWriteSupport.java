package gr.ds.unipi.spatialnodb.messages.common.points;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.util.HashMap;

public class PointWriteSupport extends WriteSupport<Point> {

    MessageType schema = MessageTypeParser.parseMessageType( "message Point {\n" +
            "required DOUBLE x;\n" +
            "required DOUBLE y;\n" +
            "required DOUBLE t;\n" +
            "required INT64 hilbertKey;\n" +
            "}");
    RecordConsumer recordConsumer;

    @Override
    public WriteContext init(Configuration configuration) {
        return new WriteContext(schema, new HashMap<>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(Point point) {
        recordConsumer.startMessage();

        recordConsumer.startField("x",0);
        recordConsumer.addDouble(point.getX());
        recordConsumer.endField("x",0);

        recordConsumer.startField("y",1);
        recordConsumer.addDouble(point.getY());
        recordConsumer.endField("y",1);

        recordConsumer.startField("t",2);
        recordConsumer.addDouble(point.getT());
        recordConsumer.endField("t",2);

        recordConsumer.startField("hilbertKey",3);
        recordConsumer.addLong(point.getHilbertKey());
        recordConsumer.endField("hilbertKey",3);

        recordConsumer.endMessage();
    }
}
