package gr.ds.unipi.spatialnodb.queries.trajparquet;

import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.CellScore;
import junit.framework.TestCase;

import java.util.Comparator;
import java.util.PriorityQueue;

public class KnnQueriesDirectoriesTest extends TestCase {

    public void testMain() {
        PriorityQueue<CellScore> queueCells = new PriorityQueue<>(Comparator.comparingDouble(CellScore::getScore));

        queueCells.add(CellScore.newCellScore(1, 85));
        queueCells.add(CellScore.newCellScore(2, 24));
        queueCells.add(CellScore.newCellScore(3, 45));

        queueCells.add(CellScore.newCellScore(4, 70));

        queueCells.add(CellScore.newCellScore(5, 5));

        queueCells.add(CellScore.newCellScore(6, 99));


        System.out.println(queueCells.poll());
    }
}