package com.oltpbenchmark.benchmarks.tpcc.executioners;

import com.oltpbenchmark.benchmarks.tpcc.TPCCExecutioner;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SSIExecutioner extends TPCCExecutioner {

    private class SSIThread extends ExecThread {

        SSIExecutioner exec;

        public SSIThread(SSIExecutioner exec, Transaction trx) {
            this.exec = exec;
            this.trx = trx;
        }

        @Override
        public void run() {

            while (true) {

                Timestamp ts = new Timestamp(System.currentTimeMillis());

                exec.register(trx);

                try {
                    this.sleep(trx.execTime);
                } catch (Exception e) {

                }

                if (commit(trx, ts)) {
                    break;
                }
            }
        }
    }

    private  final class ObjectStatus {
        public HashSet<Integer> reading = new HashSet<>();
        public HashSet<Integer> writing = new HashSet<>();
    }

    private final class SGNode {
        public Transaction trx;
        public HashSet<Integer> incoming;
        public HashSet<Integer> outgoing;

        public SGNode(Transaction trx, Lock sgLock) {
            this.trx = trx;
            incoming = new HashSet<>();
            outgoing = new HashSet<>();
        }
    }

    private HashMap<Integer, Transaction> trxList = new HashMap<>();
    private HashMap<Integer, SGNode> trxNode = new HashMap<>();
    private HashMap<String, ObjectStatus> status = new HashMap<>();
    private HashMap<String, Timestamp> ts = new HashMap<>();
    public Lock sgLock = new ReentrantLock();



    @Override
    protected ExecThread getThread(Transaction trx) {
        return new SSIThread(this, trx);
    }

    public void register(Transaction trx) {

        sgLock.lock();

        trxList.put(trx.trxID, trx);

        trx.abort = false;

        SGNode node = new SGNode(trx, sgLock);

        trxNode.put(trx.trxID, node);

        // collect dependency

        for (String obj : trx.writeSet) {

            ObjectStatus os;

            if (status.containsKey(obj)) {
                os = status.get(obj);
            } else {
                os = new ObjectStatus();
                status.put(obj, os);
            }

            for (Integer readID : os.reading) {

                if (trxList.get(readID).commit)
                    continue;
                if (trxList.get(readID).abort)
                    continue;

                node.incoming.add(readID);
                trxNode.get(readID).outgoing.add(trx.trxID);
            }

        }

        for (String obj : trx.readSet) {

            ObjectStatus os;

            if (status.containsKey(obj)) {
                os = status.get(obj);
            } else {
                os = new ObjectStatus();
                status.put(obj, os);
            }

            for (Integer writeID : os.writing) {

                if (trxList.get(writeID).commit)
                    continue;
                if (trxList.get(writeID).abort)
                    continue;

                node.outgoing.add(writeID);
                trxNode.get(writeID).incoming.add(trx.trxID);
            }
        }

        if (!node.incoming.isEmpty() && !node.outgoing.isEmpty()) {
            trx.abort = true;
            sgLock.unlock();
            return;
        }

        for (Integer readID : node.incoming) {
            for (Integer depID : trxNode.get(readID).incoming) {
                if (trxList.get(depID).commit)
                    continue;
                if (trxList.get(depID).abort)
                    continue;

                trxList.get(readID).abort = true;
                break;
            }
        }

        for (Integer writeID : node.outgoing) {
            for (Integer depID : trxNode.get(writeID).outgoing) {
                if (trxList.get(depID).commit)
                    continue;
                if (trxList.get(depID).abort)
                    continue;

                trxList.get(writeID).abort = true;
                break;
            }
        }

        // update object status

        for (String obj : trx.readSet) {

            ObjectStatus os;
            if (status.containsKey(obj)) {
                os = status.get(obj);
            } else {
                os = new ObjectStatus();
                status.put(obj, os);
            }

            os.reading.add(trx.trxID);
        }

        for (String obj : trx.writeSet) {

            ObjectStatus os;
            if (status.containsKey(obj)) {
                os = status.get(obj);
            } else {
                os = new ObjectStatus();
                status.put(obj, os);
            }

            os.writing.add(trx.trxID);
        }


        sgLock.unlock();

    }

    public boolean commit(Transaction trx, Timestamp start) {

        sgLock.lock();

        if (trx.abort) {
            clearTrx(trx);
            sgLock.unlock();
            return false;
        }

        for (String obj : trx.writeSet) {
            if (ts.containsKey(obj) && ts.get(obj).after(start)) {
                clearTrx(trx);
                sgLock.unlock();
                return false;
            }
        }

        Timestamp commitTime = new Timestamp(System.currentTimeMillis());
        for (String obj : trx.writeSet) {
            ts.put(obj, commitTime);
        }
        clearTrx(trx);
        sgLock.unlock();
        return true;

    }

    // Make sure sgLock is grabbed before enter
    public void clearTrx(Transaction trx) {
        for (Integer depID : trxNode.get(trx.trxID).incoming) {
            trxNode.get(depID).outgoing.remove(trx.trxID);
        }
        for (Integer depID : trxNode.get(trx.trxID).outgoing) {
            trxNode.get(depID).incoming.remove(trx.trxID);
        }
        trxNode.remove(trx.trxID);
        for (String obj : trx.readSet) {
            status.get(obj).reading.remove(trx.trxID);
        }
        for (String obj : trx.writeSet) {
            status.get(obj).writing.remove(trx.trxID);
        }
    }
}
