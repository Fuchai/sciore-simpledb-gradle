package simpledb.query;

public class UnionScan implements Scan {
    private Scan s1,s2;

    public UnionScan(Scan s1, Scan s2) {
        this.s1 = s1;
        this.s2 = s2;
    }

    @Override
    public void beforeFirst() {
        s1.beforeFirst();
        s2.beforeFirst();
    }

    @Override
    public boolean next() {
        if(s1.next())
            return true;
        else if (s2.next())
            return true;
        else
            return false;
    }

    @Override
    public void close() {
        s1.close();
        s2.close();
    }

    @Override
    public Constant getVal(String fldname) {
        return null;
    }

    @Override
    public int getInt(String fldname) {
        return 0;
    }

    @Override
    public String getString(String fldname) {
        return null;
    }

    @Override
    public boolean hasField(String fldname) {
        return false;
    }
}
