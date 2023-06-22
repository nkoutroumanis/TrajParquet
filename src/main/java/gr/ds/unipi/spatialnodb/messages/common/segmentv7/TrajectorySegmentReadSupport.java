package gr.ds.unipi.spatialnodb.messages.common.segmentv7;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.util.Map;

public class TrajectorySegmentReadSupport extends ReadSupport<TrajectorySegment[]> {

    public static double xmin;
    public static double ymin;
    public static long tmin;
    public static double xmax;
    public static double ymax;
    public static long tmax;

//    public final double xmin;
//    public final double ymin;
//    public final long tmin;
//    public final double xmax;
//    public final double ymax;
//    public final long tmax;

//    public TrajectorySegmentReadSupport(double xmin, double ymin, long tmin, double xmax, double ymax, long tmax){
//        this.xmin = xmin;
//        this.ymin = ymin;
//        this.tmin = tmin;
//        this.xmax = xmax;
//        this.ymax = ymax;
//        this.tmax = tmax;
//        System.out.println("XMIN: "+xmin);
//    }

    @Override
    public ReadContext init(InitContext context){
        return new ReadContext(context.getFileSchema());
    }

    @Override
    public RecordMaterializer<TrajectorySegment[]> prepareForRead(Configuration configuration, Map<String, String> map, MessageType messageType, ReadContext readContext) {
//        System.out.println("XMIN "+xmin);
        return new TrajectorySegmentMaterializer(xmin,  ymin, tmin, xmax, ymax, tmax);
    }
}
