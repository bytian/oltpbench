package com.oltpbenchmark.benchmarks.tpcc.executioners;

import com.oltpbenchmark.benchmarks.tpcc.TPCCExecutioner;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DAMVExecutioner extends TPCCExecutioner {

    private class DAMVThread extends ExecThread {

        DAMVExecutioner exec;

        public DAMVThread(DAMVExecutioner exec, Transaction trx) {
            this.exec = exec;
            this.trx = trx;
        }

        @Override
        public void run() {

            exec.register(trx);

            exec.sgLock.lock();

            while (!exec.checkRunnable(trx)) {
                try {
                    exec.trxNode.get(trx.trxID).depCommit.await();
                } catch (Exception e) {

                }
            }

            exec.sgLock.unlock();

            try {
                this.sleep(trx.execTime);
            } catch (Exception e) {

            }

            exec.commit(trx);
        }
    }

    private  final class ObjectStatus {
        public HashSet<Integer> reading = new HashSet<>();
        public Transaction writing = null;
    }

    private final class SGNode {
        public Transaction trx;
        public HashSet<Integer> dep;
        public HashSet<Integer> toSignal;
        public Condition depCommit;

        public SGNode(Transaction trx, Lock sgLock) {
            this.trx = trx;
            dep = new HashSet<>();
            toSignal = new HashSet<>();
            depCommit = sgLock.newCondition();
        }
    }

    private HashMap<Integer, Transaction> trxList = new HashMap<>();
    private HashMap<Integer, SGNode> trxNode = new HashMap<>();
    private HashMap<String, ObjectStatus> status = new HashMap<>();
    private HashMap<Integer, Boolean> committed = new HashMap<>();
    public Lock sgLock = new ReentrantLock();



    @Override
    protected ExecThread getThread(Transaction trx) {
        return new DAMVThread(this, trx);
    }

    public void register(Transaction trx) {

        sgLock.lock();

        trxList.put(trx.trxID, trx);

        SGNode node = new SGNode(trx, sgLock);

        // collect dependency

        for (String obj : trx.readSet) {

            ObjectStatus os;

            if (status.containsKey(obj)) {
                os = status.get(obj);
            } else {
                os = new ObjectStatus();
                status.put(obj, os);
            }

            if (os.writing != null) { // register wr dependency

                node.dep.add(os.writing.trxID);
                trxNode.get(os.writing.trxID).toSignal.add(trx.trxID);
            }
        }

        // update object status

        for (String obj : trx.writeSet) {

            ObjectStatus os = status.get(obj);

            os.writing = trx;
        }

        trxNode.put(trx.trxID, node);

        committed.put(trx.trxID, false);

        sgLock.unlock();

    }

    // make sure to grab sgLock before entering
    public boolean checkRunnable(Transaction trx) {

        for (Integer pred : trxNode.get(trx.trxID).dep) {
            if (!committed.get(trxList.get(pred).trxID)) {
                return false;
            }
        }

        return true;
    }

    public void commit(Transaction trx) {

        sgLock.lock();

        committed.put(trx.trxID, true);

        for (Integer succ : trxNode.get(trx.trxID).toSignal) {
            trxNode.get(trxList.get(succ).trxID).depCommit.signal();
        }

        sgLock.unlock();

    }
}
