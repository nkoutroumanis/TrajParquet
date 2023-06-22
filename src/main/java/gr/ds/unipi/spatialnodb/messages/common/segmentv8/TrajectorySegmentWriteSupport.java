package gr.ds.unipi.spatialnodb.messages.common.segmentv8;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;

public class TrajectorySegmentWriteSupport extends WriteSupport<TrajectorySegment> {

    MessageType schema = MessageTypeParser.parseMessageType( "message TrajectorySegment {\n" +
            "required BINARY objectId;\n" +
            "required INT64 segment;\n" +
            "required BINARY spt;\n" +
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

        ByteBuffer bb = ByteBuffer.allocate(trajectory.getSpatioTemporalPoints().length*3*8);
        for (SpatioTemporalPoint stPoint : trajectory.getSpatioTemporalPoints()) {
            bb.putDouble(stPoint.getLongitude());
            bb.putDouble(stPoint.getLatitude());
            bb.putLong(stPoint.getTimestamp());
        }

        recordConsumer.startField("spt",2);
        recordConsumer.addBinary(Binary.fromConstantByteArray(bb.array()));
        recordConsumer.endField("spt",2);

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
