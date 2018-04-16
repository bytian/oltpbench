package com.oltpbenchmark.benchmarks.tpcc.executioners;

import com.oltpbenchmark.benchmarks.tpcc.TPCCExecutioner;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

// Repeatable Read Execution
public class RRExecutioner extends TPCCExecutioner {
    private class RRThread extends ExecThread {

        RRExecutioner exec;

        public RRThread(RRExecutioner exec, Transaction trx) {
            this.exec = exec;
            this.trx = trx;
        }

        @Override
        public void run() {

            while (true) {

                trx.readTs = exec.getReadTs(trx.readSet);

                try {
                    this.sleep(trx.execTime);
                } catch (Exception e) {

                }

                if (validate(trx.readTs, trx.writeSet))
                    return;
            }

        }
    }


    @Override
    protected ExecThread getThread(Transaction trx) {
        return new RRExecutioner.RRThread(this, trx);
    }


    public HashMap<String, Timestamp> ts = new HashMap<String, Timestamp>();

    private Lock tsLock = new ReentrantLock();

    public HashMap<String, Timestamp> getReadTs(HashSet<String> readSet) {

        HashMap<String, Timestamp> ret = new HashMap<String, Timestamp>();

        tsLock.lock();

        for (String obj : readSet) {
            if (ts.containsKey(obj)) {
                ret.put(obj, ts.get(obj));
            } else {
                Timestamp now = new Timestamp(System.currentTimeMillis());
                ts.put(obj, now);
                ret.put(obj, now);
            }
        }

        tsLock.unlock();

        return ret;
    }

    public boolean validate(HashMap<String, Timestamp> readTs, HashSet<String> writeSet) {

        return true;
    }

}
