package gr.ds.unipi.spatialnodb.messages.common.segmentv3;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.bson.BsonBinaryWriter;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

import java.util.HashMap;

public class TrajectoryWriteSupport extends WriteSupport<Trajectory> {

    MessageType schema = MessageTypeParser.parseMessageType( "message Trajectory {\n" +
            "required BINARY objectId;\n" +
            "optional INT64 trajectoryId;\n" +
            "required BINARY linestring;\n" +
            "repeated group timestamps{\n" +
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

    //private static final BsonDocumentCodec codec = new BsonDocumentCodec();

    @Override
    public void write(Trajectory trajectory) {
        recordConsumer.startMessage();

        recordConsumer.startField("objectId",0);
        recordConsumer.addBinary(Binary.fromString(trajectory.getObjectId()));
        recordConsumer.endField("objectId",0);

        if(trajectory.getTrajectoryId()!=-1){
            recordConsumer.startField("trajectoryId",1);
            recordConsumer.addLong(trajectory.getTrajectoryId());
            recordConsumer.endField("trajectoryId",1);
        }

        BsonDocumentCodec codec = new BsonDocumentCodec();
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(buffer);
        codec.encode(writer, trajectory.getLineString().toBsonDocument(), EncoderContext.builder().isEncodingCollectibleDocument(true).build());
        //writer.close();
//        System.out.println("BSON DOC" +trajectory.getLineString().toBsonDocument());

//        System.out.println("BYTE ARRAY SIZE"+buffer.toByteArray().length);
        recordConsumer.startField("linestring",2);
        recordConsumer.addBinary(Binary.fromConstantByteArray(buffer.toByteArray()));
        recordConsumer.endField("linestring",2);
        buffer.close();

        recordConsumer.startField("timestamps",3);
        recordConsumer.startField("timestamp",0);
        for (long timestamp : trajectory.getTimestamps()) {
            recordConsumer.addLong(timestamp);
        }
        recordConsumer.endField("timestamp",0);
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
