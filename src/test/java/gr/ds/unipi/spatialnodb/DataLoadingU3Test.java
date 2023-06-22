package gr.ds.unipi.spatialnodb;

import com.uber.h3core.H3Core;
import com.uber.h3core.H3CoreLoader;
import com.uber.h3core.util.LatLng;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DataLoadingU3Test {

    @Ignore
    @Test
     public void main() throws IOException {
        H3CoreLoader.loadNatives();
        H3Core h3 = H3Core.newInstance();

        double lat = 39.153;
        double lng = -88.578;
        int res = 9;

        String hexAddr = h3.latLngToCellAddress(lat, lng, res);
//822837fffffffff
        System.out.println(hexAddr);

        //List<LatLng> LatLngs = h3.cellToBoundary(hexAddr);

        //LatLngs.forEach(f-> System.out.println(f.toString()));


        long hexLong = h3.latLngToCell(lat, lng, res);

        System.out.println(hexLong);

        System.out.println(h3.h3ToString(631210968840346111l));
        System.out.println(h3.stringToH3("8c28308280f19ff"));


        List<LatLng> shape = new ArrayList<>();
        shape.add(new LatLng(37.813318999983238, -122.4089866999972145));
        shape.add(new LatLng(37.7198061999978478, -122.3544736999993603));
        shape.add(new LatLng(37.8151571999998453, -122.4798767000009008));

        System.out.println(h3.polygonToCellAddresses(shape,new ArrayList<>(),res));

    }

    @Test
    public void numberOfCellsInSpace() throws IOException {
        H3Core h3 = H3Core.newInstance();

        int res = 8
                ;

        List<LatLng> shape = new ArrayList<>();
        shape.add(new LatLng(17.673976, -124.763068));
        shape.add(new LatLng(49.384359, -124.763068));
        shape.add(new LatLng(49.384359,-64.564908 ));
        shape.add(new LatLng(17.673976, -64.564908));
        shape.add(new LatLng(17.673976, -124.763068));


        List<List<LatLng>> holes = new ArrayList<>();
        holes.add(shape);

        System.out.println(h3.polygonToCellAddresses(shape,holes,res).size());

    }
}