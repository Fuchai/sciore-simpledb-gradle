package simpledb.query;

import simpledb.record.Schema;

public class JoinPlan implements Plan{
    private Plan p1,p2;
    private Schema schema = new Schema();
    private Predicate predicate;

    public JoinPlan(Plan p1, Plan p2, Predicate predicate) {
        this.p1 = p1;
        this.p2 = p2;
        this.predicate = predicate;
        this.schema.addAll(p1.schema());
        this.schema.addAll(p2.schema());
    }

    @Override
    public Scan open() {
        Scan s1 = p1.open();
        Scan s2 = p2.open();
        return new JoinScan(s1,s2,this.predicate);
    }

    @Override
    public int blocksAccessed() {
        Plan p= new ProductPlan(p1,p2);
        return p.blocksAccessed();
    }

    // Estimate of output number of records.
    @Override
    public int recordsOutput() {
        Plan p = new ProductPlan(p1,p2);
        return p.recordsOutput()/ predicate.reductionFactor(p);
    }

    @Override
    public int distinctValues(String fldname) {
        // what does this achieve?
        // what kind of estimate is this?
        // it cannot be calculated without actually running join
        // but running join is very expensive.
        // Since predicate truth value statistically has no correlation with
        // a particular field given no prior, we can use reduction factor.
        // However, a particular field value can be predicated upon,
        // therefore the estimate is unbiased but has high variance.

        Plan p = new ProductPlan(p1,p2);

        if (p1.schema().hasField(fldname))
            return p1.distinctValues(fldname)/predicate.reductionFactor(p);
        else
            return p2.distinctValues(fldname)/predicate.reductionFactor(p);
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
