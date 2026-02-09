package gr.ds.unipi.spatialnodb.messages.common.trajparquet;

public class CellScore {
    private final long cellId;
    private final double score;

    public CellScore(long cellId, double score) {
        this.cellId = cellId;
        this.score = score;
    }
    public long getCellId() {
        return cellId;
    }
    public double getScore() {
        return score;
    }

    public static CellScore newCellScore(long cellId, double score) {
        return new CellScore(cellId, score);
    }

}
