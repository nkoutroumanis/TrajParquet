package gr.ds.unipi.spatialnodb.messages.common.trajparquetold;

//import fi.iki.yak.ts.compression.gorilla.ByteBufferBitOutput;
//import fi.iki.yak.ts.compression.gorilla.Compressor;
//import gr.aueb.delorean.chimp.ChimpN;
//import gr.aueb.delorean.chimp.ChimpNNoIndex;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
//import gr.aueb.delorean.chimp.Chimp;

import java.nio.ByteBuffer;
import java.util.HashMap;

public class TrajectorySegmentWriteSupport extends WriteSupport<TrajectorySegment> {

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

        ByteBuffer blongitude = ByteBuffer.allocate(trajectory.getSpatioTemporalPoints().length*8);
        for (SpatioTemporalPoint stPoint : trajectory.getSpatioTemporalPoints()) {
            blongitude.putDouble(stPoint.getLongitude());
        }
        recordConsumer.startField("longitude",2);
        recordConsumer.addBinary(Binary.fromConstantByteArray(blongitude.array()));
        recordConsumer.endField("longitude",2);

        ByteBuffer blatitude = ByteBuffer.allocate(trajectory.getSpatioTemporalPoints().length*8);
        for (SpatioTemporalPoint stPoint : trajectory.getSpatioTemporalPoints()) {
            blatitude.putDouble(stPoint.getLatitude());
        }
        recordConsumer.startField("latitude",3);
        recordConsumer.addBinary(Binary.fromConstantByteArray(blatitude.array()));
        recordConsumer.endField("latitude",3);

        ByteBuffer btimestamp = ByteBuffer.allocate(trajectory.getSpatioTemporalPoints().length*8);
        for (SpatioTemporalPoint stPoint : trajectory.getSpatioTemporalPoints()) {
            btimestamp.putLong(stPoint.getTimestamp());
        }
        recordConsumer.startField("timestamps",4);
        recordConsumer.addBinary(Binary.fromConstantByteArray(btimestamp.array()));
        recordConsumer.endField("timestamps",4);

        recordConsumer.startField("minLongitude",5);
        recordConsumer.addDouble(trajectory.getMinLongitude());
        recordConsumer.endField("minLongitude",5);

        recordConsumer.startField("minLatitude",6);
        recordConsumer.addDouble(trajectory.getMinLatitude());
        recordConsumer.endField("minLatitude",6);

        recordConsumer.startField("minTimestamp",7);
        recordConsumer.addLong(trajectory.getMinTimestamp());
        recordConsumer.endField("minTimestamp",7);

        recordConsumer.startField("maxLongitude",8);
        recordConsumer.addDouble(trajectory.getMaxLongitude());
        recordConsumer.endField("maxLongitude",8);

        recordConsumer.startField("maxLatitude",9);
        recordConsumer.addDouble(trajectory.getMaxLatitude());
        recordConsumer.endField("maxLatitude",9);

        recordConsumer.startField("maxTimestamp",10);
        recordConsumer.addLong(trajectory.getMaxTimestamp());
        recordConsumer.endField("maxTimestamp",10);

        recordConsumer.endMessage();
    }
}
