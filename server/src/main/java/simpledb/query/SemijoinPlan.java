package simpledb.query;

import simpledb.record.Schema;

public class SemijoinPlan implements Plan{

    private Plan p1, p2;
    private Schema schema = new Schema();
    private Predicate predicate;

    public SemijoinPlan(Plan p1, Plan p2, Predicate predicate) {
        this.p1 = p1;
        this.p2 = p2;
        this.predicate = predicate;
        schema.addAll(p1.schema());
        schema.addAll(p2.schema());
    }

    @Override
    public Scan open() {
        Scan s1= p1.open();
        Scan s2= p2.open();
        return new SemijoinScan(s1,s2,this.predicate);
    }

    // Still, no idea what the formula is.
    // Feel like the number of accessed block depends on how many distinct values
    // there are on rhs. But I cannot use field name so I cannot know what distinct
    // value there is.
    // By expectation, the search should end halfway, so divide by 2.
    @Override
    public int blocksAccessed() {
        Plan p=new ProductPlan(p1,p2);
        return p.blocksAccessed()/2;
    }

    // No idea how the maths should be.
    @Override
    public int recordsOutput() {
        Plan p=new ProductPlan(p1,p2);
        return p1.recordsOutput()/predicate.reductionFactor(p);
    }

    @Override
    public int distinctValues(String fldname) {
        Plan p=new ProductPlan(p1,p2);
        return p1.distinctValues(fldname)/predicate.reductionFactor(p);
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
