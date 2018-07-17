package com.oltpbenchmark.benchmarks.tpcc;


import com.oltpbenchmark.benchmarks.tpcc.executioners.ExecThread;
import com.oltpbenchmark.benchmarks.tpcc.executioners.Transaction;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class TPCCExecutioner {

    private Lock logLock = new ReentrantLock();
    private BufferedWriter writer;

    {
        try {
            writer = new BufferedWriter(new FileWriter("workload.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void log(String procedureString) {
        logLock.lock();
        try {
            writer.write(procedureString);
            writer.flush();
        } catch (Exception e) {}
        logLock.unlock();
    }

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
