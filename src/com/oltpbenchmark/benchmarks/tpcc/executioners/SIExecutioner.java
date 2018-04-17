package com.oltpbenchmark.benchmarks.tpcc.executioners;

import com.oltpbenchmark.benchmarks.tpcc.TPCCExecutioner;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

// SI for serializablility
public class SIExecutioner extends TPCCExecutioner {

    private class SIThread extends ExecThread {

        SIExecutioner exec;

        public SIThread(SIExecutioner exec, Transaction trx) {
            this.exec = exec;
            this.trx = trx;
        }

        @Override
        public void run() {

            while (true) {

                Timestamp ts = new Timestamp(System.currentTimeMillis());

                try {
                    this.sleep(trx.execTime);
                } catch (Exception e) {

                }

                if (validate(trx.writeSet, ts))
                    return;
            }

        }
    }


    @Override
    protected ExecThread getThread(Transaction trx) {
        return new SIThread(this, trx);
    }


    public HashMap<String, Timestamp> ts = new HashMap<String, Timestamp>();

    private Lock tsLock = new ReentrantLock();

    public boolean validate(HashSet<String> writeSet, Timestamp start) {

        tsLock.lock();

        for (String obj : writeSet) {
            if (ts.containsKey(obj) && ts.get(obj).after(start)) {
                tsLock.unlock();
                return false;
            }
        }

        Timestamp now = new Timestamp(System.currentTimeMillis());
        for (String obj : writeSet) {
            ts.put(obj, now);
        }

        tsLock.unlock();

        return true;
    }


}
