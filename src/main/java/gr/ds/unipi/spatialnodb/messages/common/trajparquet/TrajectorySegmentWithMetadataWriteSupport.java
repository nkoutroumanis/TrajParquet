package gr.ds.unipi.spatialnodb.messages.common.trajparquet;

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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;

public class TrajectorySegmentWithMetadataWriteSupport extends WriteSupport<TrajectorySegmentWithMetadata> {

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
            "optional BINARY pivotsLongitude;\n" +
            "optional BINARY pivotsLatitude;\n" +
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
    public void write(TrajectorySegmentWithMetadata trajectory) {
        recordConsumer.startMessage();

        recordConsumer.startField("objectId",0);
        recordConsumer.addBinary(Binary.fromString(trajectory.getTrajectorySegment().getObjectId()));
        recordConsumer.endField("objectId",0);

        recordConsumer.startField("segment",1);
        recordConsumer.addLong(trajectory.getTrajectorySegment().getSegment());
        recordConsumer.endField("segment",1);

        ByteBuffer blongitude = ByteBuffer.allocate(trajectory.getTrajectorySegment().getSpatioTemporalPoints().length*8);
        for (SpatioTemporalPoint stPoint : trajectory.getTrajectorySegment().getSpatioTemporalPoints()) {
            blongitude.putDouble(stPoint.getLongitude());
        }
        recordConsumer.startField("longitude",2);
        recordConsumer.addBinary(Binary.fromConstantByteArray(blongitude.array()));
        recordConsumer.endField("longitude",2);

        ByteBuffer blatitude = ByteBuffer.allocate(trajectory.getTrajectorySegment().getSpatioTemporalPoints().length*8);
        for (SpatioTemporalPoint stPoint : trajectory.getTrajectorySegment().getSpatioTemporalPoints()) {
            blatitude.putDouble(stPoint.getLatitude());
        }
        recordConsumer.startField("latitude",3);
        recordConsumer.addBinary(Binary.fromConstantByteArray(blatitude.array()));
        recordConsumer.endField("latitude",3);

        ByteBuffer btimestamp = ByteBuffer.allocate(trajectory.getTrajectorySegment().getSpatioTemporalPoints().length*8);
        for (SpatioTemporalPoint stPoint : trajectory.getTrajectorySegment().getSpatioTemporalPoints()) {
            btimestamp.putLong(stPoint.getTimestamp());
        }
        recordConsumer.startField("timestamps",4);
        recordConsumer.addBinary(Binary.fromConstantByteArray(btimestamp.array()));
        recordConsumer.endField("timestamps",4);

        recordConsumer.startField("minLongitude",5);
        recordConsumer.addDouble(trajectory.getTrajectorySegment().getMinLongitude());
        recordConsumer.endField("minLongitude",5);

        recordConsumer.startField("minLatitude",6);
        recordConsumer.addDouble(trajectory.getTrajectorySegment().getMinLatitude());
        recordConsumer.endField("minLatitude",6);

        recordConsumer.startField("minTimestamp",7);
        recordConsumer.addLong(trajectory.getTrajectorySegment().getMinTimestamp());
        recordConsumer.endField("minTimestamp",7);

        recordConsumer.startField("maxLongitude",8);
        recordConsumer.addDouble(trajectory.getTrajectorySegment().getMaxLongitude());
        recordConsumer.endField("maxLongitude",8);

        recordConsumer.startField("maxLatitude",9);
        recordConsumer.addDouble(trajectory.getTrajectorySegment().getMaxLatitude());
        recordConsumer.endField("maxLatitude",9);

        recordConsumer.startField("maxTimestamp",10);
        recordConsumer.addLong(trajectory.getTrajectorySegment().getMaxTimestamp());
        recordConsumer.endField("maxTimestamp",10);

        if(trajectory.getPivots()!=null) {
//            if(trajectory.getTrajectorySegment().getSegment()==1) {
//                if(trajectory.getTrajectorySegment().getSpatioTemporalPoints()[0].getLongitude() != trajectory.getPivots()[trajectory.getPivots().length-1].getLongitude()
//                || trajectory.getTrajectorySegment().getSpatioTemporalPoints()[0].getLatitude() != trajectory.getPivots()[trajectory.getPivots().length-1].getLatitude()){
//                    System.exit(1);
//                }
//            }else if(trajectory.getTrajectorySegment().getSegment()<-1) {
//                if(trajectory.getTrajectorySegment().getSpatioTemporalPoints()[trajectory.getTrajectorySegment().getSpatioTemporalPoints().length-1].getLongitude() != trajectory.getPivots()[trajectory.getPivots().length-1].getLongitude()
//                        || trajectory.getTrajectorySegment().getSpatioTemporalPoints()[trajectory.getTrajectorySegment().getSpatioTemporalPoints().length-1].getLatitude() != trajectory.getPivots()[trajectory.getPivots().length-1].getLatitude()){
//                    System.exit(1);
//                }
//            }else if(trajectory.getTrajectorySegment().getSegment()==-1) {
//                if(trajectory.getPivots().length==1){
//                    System.out.println(Arrays.toString(trajectory.getTrajectorySegment().getSpatioTemporalPoints()));
//                    System.exit(1);
//                }
//
//                if(trajectory.getTrajectorySegment().getSpatioTemporalPoints()[0].getLongitude() != trajectory.getPivots()[trajectory.getPivots().length-2].getLongitude()
//                        || trajectory.getTrajectorySegment().getSpatioTemporalPoints()[0].getLatitude() != trajectory.getPivots()[trajectory.getPivots().length-2].getLatitude()){
//                    System.exit(1);
//                }
//
//                if(trajectory.getTrajectorySegment().getSpatioTemporalPoints()[trajectory.getTrajectorySegment().getSpatioTemporalPoints().length-1].getLongitude() != trajectory.getPivots()[trajectory.getPivots().length-1].getLongitude()
//                        || trajectory.getTrajectorySegment().getSpatioTemporalPoints()[trajectory.getTrajectorySegment().getSpatioTemporalPoints().length-1].getLatitude() != trajectory.getPivots()[trajectory.getPivots().length-1].getLatitude()){
//                    System.exit(1);
//                }
//            }

            ByteBuffer bPivotlongitude = ByteBuffer.allocate(trajectory.getPivots().length * 8);
            for (SpatialPoint stPoint : trajectory.getPivots()) {
                bPivotlongitude.putDouble(stPoint.getLongitude());
            }
            recordConsumer.startField("pivotsLongitude", 11);
            recordConsumer.addBinary(Binary.fromConstantByteArray(bPivotlongitude.array()));
            recordConsumer.endField("pivotsLongitude", 11);

            ByteBuffer bPivotlatitude = ByteBuffer.allocate(trajectory.getPivots().length * 8);
            for (SpatialPoint stPoint : trajectory.getPivots()) {
                bPivotlatitude.putDouble(stPoint.getLatitude());
            }
            recordConsumer.startField("pivotsLatitude", 12);
            recordConsumer.addBinary(Binary.fromConstantByteArray(bPivotlatitude.array()));
            recordConsumer.endField("pivotsLatitude", 12);

        }
        recordConsumer.endMessage();
    }
}
