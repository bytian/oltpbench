package com.oltpbenchmark.benchmarks.tpcc.executioners;

import com.oltpbenchmark.benchmarks.tpcc.TPCCExecutioner;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class OCCExecutioner extends TPCCExecutioner {

    private class OCCThread extends ExecThread {

        OCCExecutioner exec;

        public OCCThread(OCCExecutioner exec, Transaction trx) {
            this.exec = exec;
            this.trx = trx;
        }

        @Override
        public void run() {

            while (true) {

                HashMap<String, Timestamp> readTs = exec.getReadTs(trx.readSet);

                try {
                    this.sleep(trx.execTime);
                } catch (Exception e) {

                }

                if (validate(readTs, trx.writeSet))
                    return;
            }

        }
    }


    @Override
    protected ExecThread getThread(Transaction trx) {
        return new OCCThread(this, trx);
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

        tsLock.lock();

        for (String obj : readTs.keySet()) {
            if (ts.get(obj).after(readTs.get(obj))) {
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
