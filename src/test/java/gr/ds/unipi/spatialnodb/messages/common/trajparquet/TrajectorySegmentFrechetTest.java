package gr.ds.unipi.spatialnodb.messages.common.trajparquet;

import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import junit.framework.TestCase;

public class TrajectorySegmentFrechetTest extends TestCase {

    public void testFrechetOfTwoTrajectories() {

        SpatioTemporalPoint[] spt1 = new SpatioTemporalPoint[4];
//        spt1[0] = new SpatioTemporalPoint(0.2,2,0);
//        spt1[1] = new SpatioTemporalPoint(1.5,2.8,0);
//        spt1[2] = new SpatioTemporalPoint(2.3,1.6,0);
//        spt1[3] = new SpatioTemporalPoint(2.9,1.8,0);
//        spt1[4] = new SpatioTemporalPoint(4.1,3.1,0);
//        spt1[5] = new SpatioTemporalPoint(5.6,2.9,0);
//        spt1[6] = new SpatioTemporalPoint(7.2,1.3,0);
//        spt1[7] = new SpatioTemporalPoint(8.2,1.1,0);
        spt1[0] = new SpatioTemporalPoint(2,1,0);
        spt1[1] = new SpatioTemporalPoint(3,1,0);
        spt1[2] = new SpatioTemporalPoint(4,2,0);
        spt1[3] = new SpatioTemporalPoint(5,1,0);


        SpatioTemporalPoint[] spt2 = new SpatioTemporalPoint[3];
//        spt2[0] = new SpatioTemporalPoint(0.3,1.6,0);
//        spt2[1] = new SpatioTemporalPoint(3.2,3,0);
//        spt2[2] = new SpatioTemporalPoint(3.8,1.8,0);
//        spt2[3] = new SpatioTemporalPoint(5.2,3.1,0);
//        spt2[4] = new SpatioTemporalPoint(6.5,2.8,0);
//        spt2[5] = new SpatioTemporalPoint(7,0.8,0);
//        spt2[6] = new SpatioTemporalPoint(8.9,0.6,0);
        spt2[0] = new SpatioTemporalPoint(2,0,0);
        spt2[1] = new SpatioTemporalPoint(3,0,0);
        spt2[2] = new SpatioTemporalPoint(4,0,0);

        double[] arr = new double[spt1.length];
        arr[0] = HilbertUtil.euclideanDistance(spt1[0].getLongitude(), spt1[0].getLatitude(), spt2[0].getLongitude(), spt2[0].getLatitude());
        for (int i = 1; i < spt1.length; i++) {
            arr[i] = Math.max(arr[i-1],HilbertUtil.euclideanDistance(spt1[i].getLongitude(), spt1[i].getLatitude(), spt2[0].getLongitude(), spt2[0].getLatitude()));
        }

        double diagonal;
        double value;
        for (int j = 1; j < spt2.length; j++) {
            diagonal = arr[0];
            arr[0] = Math.max(HilbertUtil.euclideanDistance(spt1[0].getLongitude(), spt1[0].getLatitude(), spt2[j].getLongitude(), spt2[j].getLatitude()),diagonal);

            for (int i = 1; i < spt1.length; i++) {
                value = Math.max(HilbertUtil.euclideanDistance(spt1[i].getLongitude(), spt1[i].getLatitude(), spt2[j].getLongitude(), spt2[j].getLatitude()) ,Math.min(arr[i-1],Math.min(arr[i],diagonal)));
                diagonal = arr[i];
                arr[i] = value;
            }
        }

        System.out.println(arr[spt1.length-1]);

    }

    public void testFrechetOfTwoTrajectories1() {
        SpatioTemporalPoint[] spt1 = new SpatioTemporalPoint[14];
        spt1[0] =new SpatioTemporalPoint(-2.7383382, 49.976505, 1448624319000l);
        spt1[1] =new SpatioTemporalPoint(-2.8528934, 49.931545, 1448973221000l);
        spt1[2] =new SpatioTemporalPoint(-2.8531084, 49.931694, 1448973230000l);
        spt1[3] =new SpatioTemporalPoint(-2.853435, 49.932106, 1448973245000l);
        spt1[4] =new SpatioTemporalPoint(-2.853295, 49.94428, 1448973497000l);
        spt1[5] =new SpatioTemporalPoint(-2.85051, 49.944824, 1448973534000l);
        spt1[6] = new SpatioTemporalPoint(-2.8499718, 49.944866, 1448973539000l);
        spt1[7] =new SpatioTemporalPoint(-2.8212566, 49.94232, 1448974398000l);
        spt1[8] = new SpatioTemporalPoint(-2.8212333000000003, 49.942326, 1448974399000l);
        spt1[9] =  new SpatioTemporalPoint(-2.8296466000000002, 49.932568, 1448978032000l);
        spt1[10] =  new SpatioTemporalPoint(-2.824765, 49.944027, 1448978322000l);
        spt1[11] =  new SpatioTemporalPoint(-2.8023534, 49.95818, 1448984303000l);
        spt1[12] =  new SpatioTemporalPoint(-2.8023617, 49.958187, 1448984313000l);
        spt1[13] =  new SpatioTemporalPoint(-2.80441, 49.95868, 1448984601000l);


        SpatioTemporalPoint[] spt2 = new SpatioTemporalPoint[14];
        spt2[0] =new SpatioTemporalPoint(-2.7383382, 49.976505, 1448624319000l);
        spt2[1] =new SpatioTemporalPoint(-2.8528934, 49.931545, 1448973221000l);
        spt2[2] =new SpatioTemporalPoint(-2.8531084, 49.931694, 1448973230000l);
        spt2[3] =new SpatioTemporalPoint(-2.853435, 49.932106, 1448973245000l);
        spt2[4] =new SpatioTemporalPoint(-2.853295, 49.94428, 1448973497000l);
        spt2[5] =new SpatioTemporalPoint(-2.85051, 49.944824, 1448973534000l);
        spt2[6] = new SpatioTemporalPoint(-2.8499718, 49.944866, 1448973539000l);
        spt2[7] =new SpatioTemporalPoint(-2.8212566, 49.94232, 1448974398000l);
        spt2[8] = new SpatioTemporalPoint(-2.8212333000000003, 49.942326, 1448974399000l);
        spt2[9] =  new SpatioTemporalPoint(-2.8296466000000002, 49.932568, 1448978032000l);
        spt2[10] =  new SpatioTemporalPoint(-2.824765, 49.944027, 1448978322000l);
        spt2[11] =  new SpatioTemporalPoint(-2.8023534, 49.95818, 1448984303000l);
        spt2[12] =  new SpatioTemporalPoint(-2.8023617, 49.958187, 1448984313000l);
        spt2[13] =  new SpatioTemporalPoint(-2.80441, 49.95868, 1448984601000l);
        double[] arr = new double[spt2.length];
        arr[0] = HilbertUtil.euclideanDistance(spt1[0].getLongitude(), spt1[0].getLatitude(), spt2[0].getLongitude(), spt2[0].getLatitude());
        for (int i = 1; i < spt1.length; i++) {
            arr[i] = Math.max(arr[i-1],HilbertUtil.euclideanDistance(spt1[i].getLongitude(), spt1[i].getLatitude(), spt2[0].getLongitude(), spt2[0].getLatitude()));
        }

        double diagonal;
        double value;
        for (int j = 1; j < spt2.length; j++) {
            diagonal = arr[0];
            arr[0] = Math.max(HilbertUtil.euclideanDistance(spt1[0].getLongitude(), spt1[0].getLatitude(), spt2[j].getLongitude(), spt2[j].getLatitude()),diagonal);

            for (int i = 1; i < spt1.length; i++) {
                value = Math.max(HilbertUtil.euclideanDistance(spt1[i].getLongitude(), spt1[i].getLatitude(), spt2[j].getLongitude(), spt2[j].getLatitude()) ,Math.min(arr[i-1],Math.min(arr[i],diagonal)));
                diagonal = arr[i];
                arr[i] = value;
            }
        }

        System.out.println(arr[spt1.length-1] + " "+(Double.compare(HilbertUtil.frechetDistance(spt2,spt1),0.0)!=1));

        if (spt1.length>spt2.length){
            if(Double.compare(HilbertUtil.frechetDistance(spt2, spt1),0)!=1){
                System.out.println(true);
            }else{
                System.out.println(false);
            }
        }else{
            if(Double.compare(HilbertUtil.frechetDistance(spt1,spt2),0)!=1){
                System.out.println(true);
            }else{
                System.out.println(false);
            }
        }

        System.out.println(Double.parseDouble("-2.8212333"));
        System.out.println(Double.parseDouble("-2.8212333"));

    }

}