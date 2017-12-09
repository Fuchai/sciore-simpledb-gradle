package simpledb.query;

import simpledb.record.Schema;

import java.util.Collection;

public class RenamePlan implements Plan{
    private Plan p;
    private Collection<String> fields;
    private String oldName;
    private String newName;
    private Schema schema=new Schema();

    public RenamePlan(Plan p, String oldName, String newName) {
        this.p = p;
        this.oldName = oldName;
        this.newName = newName;
        Schema oldSchmea=p.schema();

        this.fields = oldSchmea.fields();

        for (String fldname : fields) {
            if (fldname.equals(oldName)) {
                schema.addField(newName, oldSchmea.type(oldName), oldSchmea.length(oldName));
            } else {
                schema.add(fldname, oldSchmea);
            }
        }
    }

    @Override
    public Scan open() {
        Scan s=p.open();
        return new RenameScan(s, fields,oldName,newName);
    }

    @Override
    public int blocksAccessed() {
        return p.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return p.recordsOutput();
    }

    @Override
    public int distinctValues(String fldname) {
        if (fldname.equals(newName)) {
            return p.distinctValues(oldName);
        } else {
            return p.distinctValues(fldname);
        }    }

    @Override
    public Schema schema() {
        return schema;
    }
}
