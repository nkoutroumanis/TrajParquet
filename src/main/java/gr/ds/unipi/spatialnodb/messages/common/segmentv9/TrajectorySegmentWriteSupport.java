package gr.ds.unipi.spatialnodb.messages.common.segmentv9;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.util.HashMap;

public class TrajectorySegmentWriteSupport extends WriteSupport<TrajectorySegment> {

    MessageType schema = MessageTypeParser.parseMessageType("message Geometry {\n" +
                            "required BINARY objectId;\n" +
                            "required INT64 segment;\n" +
                            "required INT32 geometryType;\n" +
                            "repeated group part {\n" +
                            "repeated group coordinate {\n" +
                            "required DOUBLE x;\n" +
                            "required DOUBLE y;\n" +
                            "required INT64 t;\n" +
                            "}\n" +
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
    public void write(TrajectorySegment trajectory) {
        recordConsumer.startMessage();

        recordConsumer.startField("objectId",0);
        recordConsumer.addBinary(Binary.fromString(trajectory.getObjectId()));
        recordConsumer.endField("objectId",0);

        recordConsumer.startField("segment",1);
        recordConsumer.addLong(trajectory.getSegment());
        recordConsumer.endField("segment",1);

        recordConsumer.startField("geometryType",2);
        recordConsumer.addInteger(3);
        recordConsumer.endField("geometryType",2);

        recordConsumer.startField("part",3);
        recordConsumer.startGroup();
        recordConsumer.startField("coordinate",0);

        for (SpatioTemporalPoint stPoint : trajectory.getSpatioTemporalPoints()) {
            recordConsumer.startGroup();
            recordConsumer.startField("x",0);
            recordConsumer.addDouble(stPoint.getLongitude());
            recordConsumer.endField("x",0);

            recordConsumer.startField("y",1);
            recordConsumer.addDouble(stPoint.getLatitude());
            recordConsumer.endField("y",1);

            recordConsumer.startField("t",2);
            recordConsumer.addLong(stPoint.getTimestamp());
            recordConsumer.endField("t",2);
            recordConsumer.endGroup();
        }

        recordConsumer.endField("coordinate",0);
        recordConsumer.endGroup();
        recordConsumer.endField("part",3);
        recordConsumer.endMessage();
    }
}
