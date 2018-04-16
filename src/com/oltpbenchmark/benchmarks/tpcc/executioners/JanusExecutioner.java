package com.oltpbenchmark.benchmarks.tpcc.executioners;

import com.oltpbenchmark.benchmarks.tpcc.TPCCExecutioner;

public class JanusExecutioner extends TPCCExecutioner {

    private class JanusThread extends ExecThread {

        JanusExecutioner exec;

        public JanusThread(JanusExecutioner exec, Transaction trx) {
            this.exec = exec;
            this.trx = trx;
        }

        @Override
        public void run() {


        }
    }

    @Override
    protected ExecThread getThread(Transaction trx) {
        return new JanusThread(this, trx);
    }
}
