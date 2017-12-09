package simpledb.query;

public class SemijoinScan implements Scan{
    private Scan prod;
    private Predicate pred;

    public SemijoinScan(Scan s1, Scan s2, Predicate pred) {
        this.prod = new ProductScan(s1, s2);
        this.pred = pred;
    }

    public void beforeFirst() {
        prod.beforeFirst();
    }

    public boolean next() {
        while (prod.next())
            if (pred.isSatisfied(prod))
                return true;
        return false;
    }

    public void close() {
        prod.close();
    }

    public Constant getVal(String fldname) {
        if (hasField(fldname)) {
            return prod.getVal(fldname);
        } else {
            throw new RuntimeException("field " + fldname + " not found.");
        }
    }
    public int getInt(String fldname) {
        if (hasField(fldname)) {
            return prod.getInt(fldname);
        } else {
            throw new RuntimeException("field " + fldname + " not found.");
        }
    }

    public String getString(String fldname) {
        if (hasField(fldname)) {
            return prod.getString(fldname);
        } else {
            throw new RuntimeException("field " + fldname + " not found.");
        }
    }

    public boolean hasField(String fldname) {
        return prod.hasField(fldname);
    }
}