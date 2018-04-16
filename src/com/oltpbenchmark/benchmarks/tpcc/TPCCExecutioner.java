package com.oltpbenchmark.benchmarks.tpcc;


import com.oltpbenchmark.benchmarks.tpcc.executioners.ExecThread;
import com.oltpbenchmark.benchmarks.tpcc.executioners.Transaction;

import java.util.HashMap;

public abstract class TPCCExecutioner {

    public void execute(String trx_type, HashMap<String, Integer> params) {

        Transaction trx = new Transaction(trx_type, params);

        ExecThread execThread = getThread(trx);

        execThread.start();

        try {
            execThread.join();
        } catch (Exception e) {

        }

    }

    protected abstract ExecThread getThread(Transaction trx);
}
