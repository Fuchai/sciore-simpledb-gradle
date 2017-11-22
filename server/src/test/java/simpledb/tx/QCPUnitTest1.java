package simpledb.tx;

import org.junit.Assert;
import simpledb.server.SimpleDB;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class QCPUnitTest1 {
    public static void main(String[] args) throws Exception {
        SimpleDB.init("studentdb");

        ArrayList<Transaction> txs=new ArrayList<>();
        for (int i = 0; i<10; i++){
            txs.add(new Transaction());
        }
        System.out.println("10 transactions");
        assert CheckpointThread.isInProgress()==true;
        assert CheckpointThread.checkpointLockAcquired ==false;
        //Quiescent started. Algorithm does not run, since there are active trans.
        for (int i=0;i<5;i++){
            txs.add(new Transaction());
        }

        TimeUnit.SECONDS.sleep(1);

        for (Transaction t: txs.subList(0,10)){
            t.commit();
        }
        //Quiescent should run.
        Assert.assertTrue(CheckpointThread.checkpointLockAcquired);
        TimeUnit.SECONDS.sleep(1);

        for (Transaction t: txs.subList(10,15)){
            t.commit();
        }


        Assert.assertFalse(CheckpointThread.checkpointLockAcquired);
        Assert.assertFalse(CheckpointThread.isInProgress());


        System.out.println("passed");

    }
}
