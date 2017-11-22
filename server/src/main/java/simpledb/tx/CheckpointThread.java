package simpledb.tx;

import simpledb.server.SimpleDB;
import simpledb.tx.recovery.CheckpointRecord;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CheckpointThread implements Runnable {

    static boolean inProgress=false;
    static boolean transactionLockacquired =false;

    private static Lock checkpointLock = new ReentrantLock();
    private static Condition noActive= checkpointLock.newCondition();

    public void run(){
        inProgress=true;
        synchronized (checkpointLock){
            while (!Transaction.getCurrentlyActiveTransactions().isEmpty()){
                try {
                    checkpointLock.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // Flush all is automatic if we wait for all the transactions to finish
                // When commit or rollback happen

                int lsn = new CheckpointRecord().writeToLog();
                SimpleDB.logMgr().flush(lsn);

                inProgress=false;
                Transaction.getTransactionLock().notifyAll();
            }
        }


//
//
//        // when a quiescent checkpoint started running, Quiescent Lock is acquired
//        // so no new transactions can be initated
//        synchronized (transactionLock){
//            // TLock acquired, stop accepting new transactions.
//            transactionLockacquired=true;
//
////            // Implemented with better technology.
////            // Lock the checkpointLock. Wait until there's no active transaction.
//            checkpointLock.lock();
//
//
//            try{
//                while(!Transaction.getCurrentlyActiveTransactions().isEmpty()){
//                    noActive.await();
//                }
//                // Now there are no active transaction. Lock acquired.
//                // Since transactionLock is also on, there is no new transaction from now on.
//
//                // Flush all is automatic if we wait for all the transactions to finish
//                // When commit or rollback happen
//
//                int lsn = new CheckpointRecord().writeToLog();
//                SimpleDB.logMgr().flush(lsn);
//
//                inProgress=false;
//
//                // no need for notifyall(), since synchronized does the same
//
//            }catch (InterruptedException e) {
//                e.printStackTrace();
//            }finally {
////                checkpointLock.unlock();
//            }
//        }
//        transactionLockacquired=false;
//        System.out.println("QCT finished");
    }

    public static Condition getNoActive() {
        return noActive;
    }

    public static boolean isInProgress() {
        return inProgress;
    }

    public static Lock getCheckpointLock() {
        return checkpointLock;
    }


    public static void setInProgress(boolean inProgress) {
        CheckpointThread.inProgress = inProgress;
    }
}
