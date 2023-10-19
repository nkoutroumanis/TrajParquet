package gr.ds.unipi.spatialnodb;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.factories.SerializerFactory;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultArraySerializers;
//import gr.aueb.delorean.chimp.Chimp;
//import gr.aueb.delorean.chimp.ChimpDecompressor;
import gr.ds.unipi.spatialnodb.messages.common.segmentv5.SpatioTemporalPoint;
import gr.ds.unipi.spatialnodb.shapes.Point;
import org.junit.Ignore;
import org.junit.Test;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Random;

public class KryoTest {

    @Ignore
    @Test
    public void main() throws IOException {
        Kryo kryo = new Kryo();

        SpatioTemporalPoint[] spatioTemporalPoints = new SpatioTemporalPoint[100000];

        Random r = new Random();

        for (int i = 0; i < spatioTemporalPoints.length; i++) {
            spatioTemporalPoints[i]=new SpatioTemporalPoint(r.nextDouble(),r.nextDouble(),r.nextLong());
        }
        System.out.println(spatioTemporalPoints[1000].getLatitude());

        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        kryo.register(SpatioTemporalPoint[].class);
        //kryo.register(SpatioTemporalPoint.class);

        Output output = new Output(1024,-1);
        kryo.writeObject(output, spatioTemporalPoints);

        byte[] we = output.toBytes();

        output.flush();

//        System.out.println(Arrays.toString(output.getBuffer()));
//        System.out.println(output.position());
//        System.out.println(Arrays.toString(output.toBytes()));

        Input input = new Input(we);

        SpatioTemporalPoint[] spt = kryo.readObject(input,SpatioTemporalPoint[].class);
        System.out.println(spt[1000].getLatitude());
    }

//    @Ignore
//    @Test
//    public void chimpTest() throws IOException {
//
//        Chimp compressorLon = new Chimp();
//        compressorLon.addValue(1231.231);
//        compressorLon.addValue(1324.344432);
//        compressorLon.addValue(892.34);
//        compressorLon.addValue(234.2);
//        compressorLon.addValue(3457.45);
//
//        compressorLon.close();
//
//        ChimpDecompressor chimpDecompressor = new ChimpDecompressor(compressorLon.getOut());
//        chimpDecompressor.getValues().forEach(System.out::println);
//
//    }

}