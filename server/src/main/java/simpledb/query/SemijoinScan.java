package simpledb.query;

public class SemijoinScan implements Scan {
    private Scan s1, s2;
    private Predicate predicate;

    public SemijoinScan(Scan s1, Scan s2, Predicate predicate) {
        this.s1 = s1;
        this.s2 = s2;
        this.predicate = predicate;
    }

    @Override
    public void beforeFirst() {
        s1.beforeFirst();
    }

    /**
     * Move the cursor to the next semijoin value.
     * @return
     */
    @Override
    public boolean next() {
        s2.beforeFirst();
        while(s1.next()) {
            while (s2.next()) {
                if (predicate.isSatisfied(this)) {
                    return true;
                    // next time the function is called, a new lhs will be used,
                    // and s2 will be as new.
                }
            }
        }
        return false;
    }

    @Override
    public void close() {
        s1.close();
        s2.close();
    }

    @Override
    public Constant getVal(String fldname) {
        return s1.getVal(fldname);
    }

    @Override
    public int getInt(String fldname) {
        return s1.getInt(fldname);
    }

    @Override
    public String getString(String fldname) {
        return s1.getString(fldname);
    }

    @Override
    public boolean hasField(String fldname) {
        return s1.hasField(fldname);
    }
}