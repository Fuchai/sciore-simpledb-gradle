package simpledb.tx;

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

        //wait a second
        TimeUnit.SECONDS.sleep(2);

        for (Transaction t: txs.subList(10,15)){
            t.commit();
        }


        System.out.println("passed");

    }
}
