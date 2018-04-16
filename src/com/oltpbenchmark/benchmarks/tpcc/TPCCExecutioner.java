package com.oltpbenchmark.benchmarks.tpcc;


import com.oltpbenchmark.benchmarks.tpcc.executioners.ExecThread;
import com.oltpbenchmark.benchmarks.tpcc.executioners.Transaction;

import java.util.HashSet;

public abstract class TPCCExecutioner {

    public void execute(String trxType, HashSet<String> readSet, HashSet<String> writeSet) {

        Transaction trx = new Transaction(trxType, readSet, writeSet);

        ExecThread execThread = getThread(trx);

        execThread.start();

        try {
            execThread.join();
        } catch (Exception e) {

        }

    }

    protected abstract ExecThread getThread(Transaction trx);
}
