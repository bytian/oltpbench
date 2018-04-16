package com.oltpbenchmark.benchmarks.tpcc.executioners;

import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;

import java.util.HashSet;
import java.util.HashMap;

public final class Transaction {
    public HashSet<String> readSet = new HashSet<String>();
    public HashSet<String> writeSet = new HashSet<>();
    public int execTime;

    public Transaction(String trx_type, HashMap<String, Integer> params) {
        switch (trx_type) {
            case TPCCWorker.NewOrderTrx:
                readSet.add("W," + Integer.toString(params.get("W_ID")));
                readSet.add("D," + Integer.toString(params.get("D_W_ID")) + ","
                        + Integer.toString(params.get("D_ID")));
                writeSet.add("D," + Integer.toString(params.get("D_W_ID")) + ","
                        + Integer.toString(params.get("D_ID")));
                readSet.add("C," + Integer.toString(params.get("C_W_ID")) + ","
                        + Integer.toString(params.get("C_D_ID")) + ","
                        + Integer.toString(params.get("C_ID")));
                readSet.add("I," + Integer.toString(params.get("I_ID")));
                readSet.add("S," + Integer.toString(params.get("S_I_ID")) + ","
                        + Integer.toString(params.get("S_W_ID")));
                writeSet.add("S," + Integer.toString(params.get("S_I_ID")) + ","
                        + Integer.toString(params.get("S_W_ID")));
                execTime = newOrderTime;
                break;
            case TPCCWorker.PaymentTrx:
                readSet.add("W," + Integer.toString(params.get("W_ID")));
                writeSet.add("W," + Integer.toString(params.get("W_ID")));
                readSet.add("D," + Integer.toString(params.get("D_W_ID")) + ","
                        + Integer.toString(params.get("D_ID")));
                if (params.containsKey("C_ID")) {
                    readSet.add("C," + Integer.toString(params.get("C_W_ID")) + ","
                            + Integer.toString(params.get("C_D_ID")) + ","
                            + Integer.toString(params.get("C_ID")));
                    writeSet.add("C," + Integer.toString(params.get("C_W_ID")) + ","
                            + Integer.toString(params.get("C_D_ID")) + ","
                            + Integer.toString(params.get("C_ID")));
                }
                execTime = paymentTime;
                break;
            case TPCCWorker.OrderStatusTrx:
                if (params.containsKey("C_ID")) {
                    readSet.add("C," + Integer.toString(params.get("C_W_ID")) + ","
                            + Integer.toString(params.get("C_D_ID")) + ","
                            + Integer.toString(params.get("C_ID")));
                }
                execTime = orderStatusTime;
                break;
            case TPCCWorker.DeliveryTrx:
                readSet.add("O," + Integer.toString(params.get("O_W_ID")) + ","
                        + Integer.toString(params.get("O_D_ID")) + ","
                        + Integer.toString(params.get("O_ID")));
                writeSet.add("O," + Integer.toString(params.get("O_W_ID")) + ","
                        + Integer.toString(params.get("O_D_ID")) + ","
                        + Integer.toString(params.get("O_ID")));
                execTime = deliveryTime;
                break;
            case TPCCWorker.StockLevelTrx:
                readSet.add("D," + Integer.toString(params.get("D_W_ID")) + ","
                        + Integer.toString(params.get("D_ID")));
                readSet.add("S," + Integer.toString(params.get("S_I_ID")) + ","
                        + Integer.toString(params.get("S_W_ID")));
                execTime = stockLevelTime;
                break;

        }
    }

    private final static int newOrderTime = 16;
    private final static int paymentTime = 4;
    private final static int orderStatusTime = 3;
    private final static int deliveryTime = 28;
    private final static int stockLevelTime = 3;

}
