/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.benchmarks.tpcc.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Random;

import com.oltpbenchmark.benchmarks.tpcc.*;
import org.apache.log4j.Logger;

import com.oltpbenchmark.api.SQLStmt;

public class Delivery extends TPCCProcedure {

//    private static final Logger LOG = Logger.getLogger(Delivery.class);
//
	public SQLStmt delivGetOrderIdSQL = new SQLStmt(
	        "SELECT NO_O_ID FROM " + TPCCConstants.TABLENAME_NEWORDER +
	        " WHERE NO_D_ID = ? " +
	        "   AND NO_W_ID = ? " +
	        " ORDER BY NO_O_ID ASC " +
	        " LIMIT 1");

	public SQLStmt delivDeleteNewOrderSQL = new SQLStmt(
	        "DELETE FROM " + TPCCConstants.TABLENAME_NEWORDER +
			" WHERE NO_O_ID = ? " +
            "   AND NO_D_ID = ?" +
			"   AND NO_W_ID = ?");

	public SQLStmt delivGetCustIdSQL = new SQLStmt(
	        "SELECT O_C_ID FROM " + TPCCConstants.TABLENAME_OPENORDER +
	        " WHERE O_ID = ? " +
            "   AND O_D_ID = ? " +
	        "   AND O_W_ID = ?");

	public SQLStmt delivUpdateCarrierIdSQL = new SQLStmt(
	        "UPDATE " + TPCCConstants.TABLENAME_OPENORDER +
	        "   SET O_CARRIER_ID = ? " +
			" WHERE O_ID = ? " +
	        "   AND O_D_ID = ?" +
			"   AND O_W_ID = ?");

	public SQLStmt delivUpdateDeliveryDateSQL = new SQLStmt(
	        "UPDATE " + TPCCConstants.TABLENAME_ORDERLINE +
	        "   SET OL_DELIVERY_D = ? " +
			" WHERE OL_O_ID = ? " +
			"   AND OL_D_ID = ? " +
			"   AND OL_W_ID = ? ");

	public SQLStmt delivSumOrderAmountSQL = new SQLStmt(
	        "SELECT SUM(OL_AMOUNT) AS OL_TOTAL " +
			"  FROM " + TPCCConstants.TABLENAME_ORDERLINE +
			" WHERE OL_O_ID = ? " +
			"   AND OL_D_ID = ? " +
			"   AND OL_W_ID = ?");

	public SQLStmt delivUpdateCustBalDelivCntSQL = new SQLStmt(
	        "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER +
	        "   SET C_BALANCE = C_BALANCE + ?," +
			"       C_DELIVERY_CNT = C_DELIVERY_CNT + 1 " +
			" WHERE C_W_ID = ? " +
			"   AND C_D_ID = ? " +
			"   AND C_ID = ? ");


//	// Delivery Txn
//	private PreparedStatement delivGetOrderId = null;
//	private PreparedStatement delivDeleteNewOrder = null;
//	private PreparedStatement delivGetCustId = null;
//	private PreparedStatement delivUpdateCarrierId = null;
//	private PreparedStatement delivUpdateDeliveryDate = null;
//	private PreparedStatement delivSumOrderAmount = null;
//	private PreparedStatement delivUpdateCustBalDelivCnt = null;


    public ResultSet run(Connection conn, Random gen,
			int w_id, int numWarehouses,
			int terminalDistrictLowerID, int terminalDistrictUpperID,
			TPCCWorker w) throws SQLException {
		
//        boolean trace = LOG.isDebugEnabled();
        int o_carrier_id = TPCCUtil.randomNumber(1, 10, gen);
        Timestamp timestamp = w.getBenchmarkModule().getTimestamp(System.currentTimeMillis());

		int d_id, c_id;
        float ol_total = 0;
        int[] orderIDs;

        String procedureString = "Delivery Transaction: \n";

        orderIDs = new int[10];
        for (d_id = 1; d_id <= terminalDistrictUpperID; d_id++) {

            procedureString += "\t" +
                    "SELECT NO_O_ID FROM " + TPCCConstants.TABLENAME_NEWORDER +
                    " WHERE NO_D_ID = " + Integer.toString(d_id) +
                    "   AND NO_W_ID = " + Integer.toString(w_id) +
                    " ORDER BY NO_O_ID ASC " +
                    " LIMIT 1\n";

            procedureString += "\t" +
                    "DELETE FROM " + TPCCConstants.TABLENAME_NEWORDER +
                    " WHERE NO_O_ID = ?" +
                    "   AND NO_D_ID = " + Integer.toString(d_id) +
                    "   AND NO_W_ID = " + Integer.toString(w_id) + "\n";

            procedureString += "\t" +
                    "SELECT O_C_ID FROM " + TPCCConstants.TABLENAME_OPENORDER +
                    " WHERE O_ID = ?" +
                    "   AND O_D_ID = " + Integer.toString(d_id) +
                    "   AND O_W_ID = " + Integer.toString(w_id) + "\n";

            procedureString += "\t" +
                    "UPDATE " + TPCCConstants.TABLENAME_OPENORDER +
                    "   SET O_CARRIER_ID =" + Integer.toString(o_carrier_id) +
                    " WHERE O_ID = ?" +
                    "   AND O_D_ID = " + Integer.toString(d_id) +
                    "   AND O_W_ID = " + Integer.toString(w_id) + "\n";

            procedureString += "\t" +
                    "UPDATE " + TPCCConstants.TABLENAME_ORDERLINE +
                    "   SET OL_DELIVERY_D = " + timestamp.toString() +
                    " WHERE OL_O_ID = ?" +
                    "   AND OL_D_ID = " + Integer.toString(d_id) +
                    "   AND OL_W_ID = " + Integer.toString(w_id) + "\n";

            procedureString += "\t" +
                    "SELECT SUM(OL_AMOUNT) AS OL_TOTAL " +
                    "  FROM " + TPCCConstants.TABLENAME_ORDERLINE +
                    " WHERE OL_O_ID = ?" +
                    "   AND OL_D_ID = " + Integer.toString(d_id) +
                    "   AND OL_W_ID = " + Integer.toString(w_id) + "\n";

            procedureString += "\t" +
                    "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER +
                    "   SET C_BALANCE = C_BALANCE + ?," +
                    "       C_DELIVERY_CNT = C_DELIVERY_CNT + 1 " +
                    " WHERE C_W_ID = " + Integer.toString(w_id) +
                    "   AND C_D_ID = " + Integer.toString(d_id) +
                    "   AND C_ID = ?\n";
        }

        TPCCBenchmark.exec.log(procedureString);

		return null;
    }

}
