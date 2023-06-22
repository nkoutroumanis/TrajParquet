package gr.ds.unipi.spatialnodb.messages.common.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.util.HashMap;

public class LineWriteSupport extends WriteSupport<Line> {

    MessageType schema = MessageTypeParser.parseMessageType( "message Line {\n" +
            "required DOUBLE minX;\n" +
            "required DOUBLE maxX;\n" +
            "required DOUBLE minY;\n" +
            "required DOUBLE maxY;\n" +
            "repeated group co{\n" +
            "required DOUBLE vals;\n" +
            "}\n" +
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
    public void write(Line line) {
        recordConsumer.startMessage();

        recordConsumer.startField("minX",0);
        recordConsumer.addDouble(line.getMinX());
        recordConsumer.endField("minX",0);

        recordConsumer.startField("maxX",1);
        recordConsumer.addDouble(line.getMaxX());
        recordConsumer.endField("maxX",1);

        recordConsumer.startField("minY",2);
        recordConsumer.addDouble(line.getMinY());
        recordConsumer.endField("minY",2);

        recordConsumer.startField("maxY",3);
        recordConsumer.addDouble(line.getMaxY());
        recordConsumer.endField("maxY",3);

        recordConsumer.startField("co",4);

        recordConsumer.startField("vals",0);
        for (double val : line.getVals()) {
            recordConsumer.addDouble(val);
        }
        recordConsumer.endField("vals",0);


        recordConsumer.endField("co",4);

        recordConsumer.endMessage();
    }
}
