package simpledb.query;

import org.junit.*;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class RenameScanTest {

    public RenameScanTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        SimpleDB.init("studentdb");
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of open method, of class RenamePlan.
     */
    @Test
    public void TestRename() {
        System.out.println("RENAME");
        Transaction tx = new Transaction();
        Plan deptTblPlan = new TablePlan("dept", tx);
        tx.commit();
        Plan rPlan = new RenamePlan(deptTblPlan, "did", "asdfasdf");
        Scan rScan = rPlan.open();
        ArrayList<String> fields = new ArrayList();
        fields.add("did");
        fields.add("dname");
        Plan projectPlan = new ProjectPlan(deptTblPlan, fields);
        Scan projectScan = projectPlan.open();
        assertEquals(true, rScan.hasField("asdfasdf"));
        assertEquals(false, projectScan.hasField("asdfasdf"));
        int pRecords = 0;
        int rRecords = 0;
        while (projectScan.next()) {
            pRecords++;
        }
        while(rScan.next()) {
            rRecords++;
        }
        assertEquals(pRecords, rRecords);
    }
}