package gr.ds.unipi.spatialnodb.messages.common.geoparquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.locationtech.jts.io.WKBWriter;

import java.util.HashMap;

public class TrajectoryWriteSupport extends WriteSupport<Trajectory> {

    MessageType schema = MessageTypeParser.parseMessageType( "message TrajectorySegment {\n" +
            "required BINARY objectId;\n" +
            "required INT64 trajectoryId;\n" +
            "required BINARY wkb;\n" +
//            "repeated group timestamps{\n" +
//            "required INT64 timestamp;\n" +
//            "}\n" +
            "repeated INT64 timestamps;\n" +
            "required DOUBLE minLongitude;\n" +
            "required DOUBLE minLatitude;\n" +
            "required INT64 minTimestamp;\n" +
            "required DOUBLE maxLongitude;\n" +
            "required DOUBLE maxLatitude;\n" +
            "required INT64 maxTimestamp;\n" +
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
    public void write(Trajectory trajectory) {
        recordConsumer.startMessage();

        recordConsumer.startField("objectId",0);
        recordConsumer.addBinary(Binary.fromString(trajectory.getObjectId()));
        recordConsumer.endField("objectId",0);

            recordConsumer.startField("trajectoryId",1);
            recordConsumer.addLong(trajectory.getTrajectoryId());
            recordConsumer.endField("trajectoryId",1);


        recordConsumer.startField("wkb",2);
        WKBWriter wkb = new WKBWriter();
        recordConsumer.addBinary(Binary.fromConstantByteArray(wkb.write(trajectory.getLineString())));
        recordConsumer.endField("wkb",2);

        recordConsumer.startField("timestamps",3);
//        recordConsumer.startGroup();
//        recordConsumer.startField("timestamp",0);
        for (long timestamp : trajectory.getTimestamps()) {
            recordConsumer.addLong(timestamp);
        }
//        recordConsumer.endField("timestamp",0);
//        recordConsumer.endGroup();
        recordConsumer.endField("timestamps",3);

        recordConsumer.startField("minLongitude",4);
        recordConsumer.addDouble(trajectory.getMinLongitude());
        recordConsumer.endField("minLongitude",4);

        recordConsumer.startField("minLatitude",5);
        recordConsumer.addDouble(trajectory.getMinLatitude());
        recordConsumer.endField("minLatitude",5);

        recordConsumer.startField("minTimestamp",6);
        recordConsumer.addLong(trajectory.getMinTimestamp());
        recordConsumer.endField("minTimestamp",6);

        recordConsumer.startField("maxLongitude",7);
        recordConsumer.addDouble(trajectory.getMaxLongitude());
        recordConsumer.endField("maxLongitude",7);

        recordConsumer.startField("maxLatitude",8);
        recordConsumer.addDouble(trajectory.getMaxLatitude());
        recordConsumer.endField("maxLatitude",8);

        recordConsumer.startField("maxTimestamp",9);
        recordConsumer.addLong(trajectory.getMaxTimestamp());
        recordConsumer.endField("maxTimestamp",9);

        recordConsumer.endMessage();
    }
}
