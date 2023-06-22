package gr.ds.unipi.spatialnodb.messages.common.segmentv7;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.util.HashMap;

public class TrajectorySegmentWriteSupport extends WriteSupport<TrajectorySegment> {

    MessageType schema = MessageTypeParser.parseMessageType( "message TrajectorySegment {\n" +
            "required BINARY objectId;\n" +
            "required INT64 segment;\n" +

            "repeated group spatioTemporalPoints{\n" +
            "required DOUBLE longitude;\n" +
            "required DOUBLE latitude;\n" +
            "required INT64 timestamp;\n" +
            "}\n" +

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
    public void write(TrajectorySegment trajectory) {
        recordConsumer.startMessage();
        recordConsumer.startField("objectId",0);
        recordConsumer.addBinary(Binary.fromString(trajectory.getObjectId()));
        recordConsumer.endField("objectId",0);

        recordConsumer.startField("segment",1);
        recordConsumer.addLong(trajectory.getSegment());
        recordConsumer.endField("segment",1);

        recordConsumer.startField("spatioTemporalPoints",2);
        for (SpatioTemporalPoint spatioTemporalPoint : trajectory.getSpatioTemporalPoints()) {
            recordConsumer.startGroup();

            recordConsumer.startField("longitude",0);
            recordConsumer.addDouble(spatioTemporalPoint.getLongitude());
            recordConsumer.endField("longitude",0);

            recordConsumer.startField("latitude",1);
            recordConsumer.addDouble(spatioTemporalPoint.getLatitude());
            recordConsumer.endField("latitude",1);

            recordConsumer.startField("timestamp",2);
            recordConsumer.addLong(spatioTemporalPoint.getTimestamp());
            recordConsumer.endField("timestamp",2);

            recordConsumer.endGroup();
        }
        recordConsumer.endField("spatioTemporalPoints",2);

        recordConsumer.startField("minLongitude",3);
        recordConsumer.addDouble(trajectory.getMinLongitude());
        recordConsumer.endField("minLongitude",3);

        recordConsumer.startField("minLatitude",4);
        recordConsumer.addDouble(trajectory.getMinLatitude());
        recordConsumer.endField("minLatitude",4);

        recordConsumer.startField("minTimestamp",5);
        recordConsumer.addLong(trajectory.getMinTimestamp());
        recordConsumer.endField("minTimestamp",5);

        recordConsumer.startField("maxLongitude",6);
        recordConsumer.addDouble(trajectory.getMaxLongitude());
        recordConsumer.endField("maxLongitude",6);

        recordConsumer.startField("maxLatitude",7);
        recordConsumer.addDouble(trajectory.getMaxLatitude());
        recordConsumer.endField("maxLatitude",7);

        recordConsumer.startField("maxTimestamp",8);
        recordConsumer.addLong(trajectory.getMaxTimestamp());
        recordConsumer.endField("maxTimestamp",8);

        recordConsumer.endMessage();
    }
}
