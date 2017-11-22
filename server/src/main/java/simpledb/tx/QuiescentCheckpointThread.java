package simpledb.tx;

import simpledb.server.SimpleDB;
import simpledb.tx.recovery.CheckpointRecord;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class QuiescentCheckpointThread extends Thread {

    static boolean inProgress=false;

    private static Object TransactionLock =new Object();

    private static Lock checkpointLock = new ReentrantLock();
    private static Condition noActive= checkpointLock.newCondition();

    public QuiescentCheckpointThread() {
    }

    public void run(){
        // when a quiescent checkpoint started running, Quiescent Lock is acquired
        // so no new transactions can be initated
        synchronized (TransactionLock){
            // TLock acquired, stop accepting new transactions.

            // Implemented with better technology.
            // Lock the checkpointLock. Wait until there's no active transaction.
            checkpointLock.lock();
            try{
                while(!Transaction.getCurrentlyActiveTransactions().isEmpty()){
                    noActive.await();
                }
                // Now there are no active transaction. Lock acquired.
                // Since TransactionLock is also on, there is no new transaction from now on.

                // Flush all is automatic if we wait for all the transactions to finish
                // When commit or rollback happen

                int lsn = new CheckpointRecord().writeToLog();
                SimpleDB.logMgr().flush(lsn);

                inProgress=false;

                // no need for notifyall(), since synchronized does the same

            }catch (InterruptedException e) {
                e.printStackTrace();
            }finally {
                checkpointLock.unlock();
            }
        }
    }

//    private static void checkFinish() throws NoActiveTransactionException {
//        if (Transaction.getCurrentlyActiveTransactions().isEmpty())
//            throw new NoActiveTransactionException("Finished all");
//
//    }

    public static Object getTransactionLock() {
        return TransactionLock;
    }

    public static Condition getNoActive() {
        return noActive;
    }

    public static boolean isInProgress() {
        return inProgress;
    }

    public static void setInProgress(boolean inProgress) {
        QuiescentCheckpointThread.inProgress = inProgress;
    }
}
