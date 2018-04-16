package com.oltpbenchmark.benchmarks.tpcc.executioners;

public abstract class ExecThread extends Thread {

    Transaction trx;

    public void init(Transaction trx) {
        this.trx = trx;
    }
}
