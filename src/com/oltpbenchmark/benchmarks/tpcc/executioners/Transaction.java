package com.oltpbenchmark.benchmarks.tpcc.executioners;

import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.HashMap;

public final class Transaction {
    public HashSet<String> readSet;
    public HashSet<String> writeSet;
    public HashMap<String, Timestamp> readTs = new HashMap<>();
    public int execTime;

    public Transaction(String trxType, HashSet<String> readSet, HashSet<String> writeSet) {
        switch (trxType) {
            case TPCCWorker.NewOrderTrx:
                execTime = newOrderTime;
                break;
            case TPCCWorker.PaymentTrx:
                execTime = paymentTime;
                break;
            case TPCCWorker.OrderStatusTrx:
                execTime = orderStatusTime;
                break;
            case TPCCWorker.DeliveryTrx:
                execTime = deliveryTime;
                break;
            case TPCCWorker.StockLevelTrx:
                execTime = stockLevelTime;
                break;

        }
        this.readSet = readSet;
        this.writeSet = writeSet;
    }

    private final static int newOrderTime = 16;
    private final static int paymentTime = 4;
    private final static int orderStatusTime = 3;
    private final static int deliveryTime = 28;
    private final static int stockLevelTime = 3;

}
