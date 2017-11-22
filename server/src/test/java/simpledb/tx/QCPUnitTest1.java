package simpledb.tx;

import simpledb.server.SimpleDB;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QCPUnitTest1 {
    public static void main(String[] args) throws Exception {
        SimpleDB.init("studentdb");

        ArrayList<Transaction> txs=new ArrayList<>();
        for (int i = 0; i<10; i++){
            txs.add(new Transaction());
        }
        System.out.println("10 transactions");
        assert CheckpointThread.isInProgress()==true;
        assert CheckpointThread.transactionLockacquired==false;
        //Quiescent started. Algorithm does not run, since there are active trans.
        for (int i=0;i<5;i++){
            txs.add(new Transaction());
        }

        TimeUnit.SECONDS.sleep(1);

        for (Transaction t: txs.subList(0,10)){
            t.commit();
        }
        //Quiescent should run.
        TimeUnit.SECONDS.sleep(2);
        assertTrue (CheckpointThread.transactionLockacquired);

        //wait a second
        TimeUnit.SECONDS.sleep(2);
        assertFalse(CheckpointThread.inProgress);
        assertTrue (CheckpointThread.transactionLockacquired);

        for (Transaction t: txs.subList(10,15)){
            t.commit();
        }

        assert CheckpointThread.transactionLockacquired==false;

        System.out.println("passed");

    }
}
