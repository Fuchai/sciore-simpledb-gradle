package simpledb.query;

public class JoinScan implements Scan{
    private Scan productScan;
    private Predicate predicate;

    public JoinScan(Scan s1, Scan s2, Predicate predicate) {
        this.productScan=new ProductScan(s1,s2);
        this.predicate=predicate;
    }

    @Override
    public void beforeFirst() {
        productScan.beforeFirst();
    }

    @Override
    public boolean next() {
        while(productScan.next()){
            if (predicate.isSatisfied(productScan)){
                return true;
            }
        }
        return false;
    }

    @Override
    public void close() {
        productScan.close();
    }

    @Override
    public Constant getVal(String fldname) {
        return productScan.getVal(fldname);
    }

    @Override
    public int getInt(String fldname) {
        return productScan.getInt(fldname);
    }

    @Override
    public String getString(String fldname) {
        return productScan.getString(fldname);
    }

    @Override
    public boolean hasField(String fldname) {
        return productScan.hasField(fldname);
    }
}
