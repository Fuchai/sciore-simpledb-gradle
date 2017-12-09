package simpledb.query;

public class RenameScan implements Scan {
    private Scan s;
    private String[] fields;
    private String oldName;
    private String newName;

    public RenameScan(Scan s, String[] fields, String oldName, String newName) {
        this.s=s;
        this.fields=fields;
        this.oldName=oldName;
        this.newName=newName;
    }

    @Override
    public void beforeFirst() {
        s.beforeFirst();
    }

    @Override
    public boolean next() {
        return s.next();
    }

    @Override
    public void close() {
        s.close();
    }

    @Override
    public Constant getVal(String fldname) {
        // I hope the lower implementation takes care of fieldname not found.
        // Should verify that.
        if (fldname.equals(newName)){
            return s.getVal(oldName);
        }else{
            return s.getVal(fldname);
        }
    }

    @Override
    public int getInt(String fldname) {
        if (fldname.equals(newName)){
            return s.getInt(oldName);
        }else{
            return s.getInt(fldname);
        }
    }

    @Override
    public String getString(String fldname) {
        if (fldname.equals(newName)){
            return s.getString(oldName);
        }else{
            return s.getString(fldname);
        }
    }

    @Override
    public boolean hasField(String fldname) {
        if (fldname.equals(newName)){
            return s.hasField(oldName);
        }else{
            return s.hasField(fldname);
        }
    }
}
