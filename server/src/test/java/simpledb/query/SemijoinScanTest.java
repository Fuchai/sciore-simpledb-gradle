package simpledb.query;

import org.junit.Before;
import org.junit.Test;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import static org.junit.Assert.assertEquals;

public class SemijoinScanTest {

    @Before
    public void setUp() throws Exception {
        SimpleDB.init("studentdb");
    }

    @Test
    public void testSemijoinScan() throws Exception {
        System.out.println("SEMIJOIN");
        Transaction tx = new Transaction();
        Plan studentTblPlan = new TablePlan("student", tx);
        Plan deptTblPlan = new TablePlan("dept", tx);
        tx.commit();
        Plan semijoinPlan = new SemijoinPlan(studentTblPlan, deptTblPlan,
                new Predicate(
                        new Term(
                                new FieldNameExpression("majorid"),
                                new FieldNameExpression("did"))));
        Scan semijoinScan = semijoinPlan.open();

        int records = 0;
        while (semijoinScan.next()) {
            for (String field: semijoinPlan.schema().fields()) {
                System.out.printf("%10s", semijoinScan.getVal(field).toString());
            }
            System.out.println();
            records++;
        }
        assertEquals(9, records);

    }

}