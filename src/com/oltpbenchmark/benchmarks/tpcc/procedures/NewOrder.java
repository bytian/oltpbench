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
import java.util.HashSet;
import java.util.Random;

import com.oltpbenchmark.benchmarks.tpcc.*;
import org.apache.log4j.Logger;

import com.oltpbenchmark.api.SQLStmt;

public class NewOrder extends TPCCProcedure {

//     private static final Logger LOG = Logger.getLogger(NewOrder.class);
//
    public final SQLStmt stmtGetCustSQL = new SQLStmt(
    		"SELECT C_DISCOUNT, C_LAST, C_CREDIT" +
	        "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
	        " WHERE C_W_ID = w_id " +
	        "   AND C_D_ID = d_id " +
	        "   AND C_ID = c_id");

    public final SQLStmt stmtGetWhseSQL = new SQLStmt(
    		"SELECT W_TAX " +
		    "  FROM " + TPCCConstants.TABLENAME_WAREHOUSE +
		    " WHERE W_ID = ?");

    public final SQLStmt stmtGetDistSQL = new SQLStmt(
    		"SELECT D_NEXT_O_ID, D_TAX " +
	        "  FROM " + TPCCConstants.TABLENAME_DISTRICT +
	        " WHERE D_W_ID = ? AND D_ID = ? FOR UPDATE");

 	public final SQLStmt  stmtInsertNewOrderSQL = new SQLStmt(
 	        "INSERT INTO " + TPCCConstants.TABLENAME_NEWORDER +
 	        " (NO_O_ID, NO_D_ID, NO_W_ID) " +
             " VALUES ( ?, ?, ?)");

 	public final SQLStmt  stmtUpdateDistSQL = new SQLStmt(
 	        "UPDATE " + TPCCConstants.TABLENAME_DISTRICT +
 	        "   SET D_NEXT_O_ID = D_NEXT_O_ID + 1 " +
             " WHERE D_W_ID = ? " +
 	        "   AND D_ID = ?");

 	public final SQLStmt  stmtInsertOOrderSQL = new SQLStmt(
 	        "INSERT INTO " + TPCCConstants.TABLENAME_OPENORDER +
 	        " (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)" +
             " VALUES (?, ?, ?, ?, ?, ?, ?)");

 	public final SQLStmt  stmtGetItemSQL = new SQLStmt(
 	        "SELECT I_PRICE, I_NAME , I_DATA " +
             "  FROM " + TPCCConstants.TABLENAME_ITEM +
             " WHERE I_ID = ?");

 	public final SQLStmt  stmtGetStockSQL = new SQLStmt(
 	        "SELECT S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, " +
             "       S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10" +
             "  FROM " + TPCCConstants.TABLENAME_STOCK +
             " WHERE S_I_ID = ? " +
             "   AND S_W_ID = ? FOR UPDATE");

 	public final SQLStmt  stmtUpdateStockSQL = new SQLStmt(
 	        "UPDATE " + TPCCConstants.TABLENAME_STOCK +
 	        "   SET S_QUANTITY = ? , " +
             "       S_YTD = S_YTD + ?, " +
 	        "       S_ORDER_CNT = S_ORDER_CNT + 1, " +
             "       S_REMOTE_CNT = S_REMOTE_CNT + ? " +
 	        " WHERE S_I_ID = ? " +
             "   AND S_W_ID = ?");

 	public final SQLStmt  stmtInsertOrderLineSQL = new SQLStmt(
 	        "INSERT INTO " + TPCCConstants.TABLENAME_ORDERLINE +
 	        " (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) " +
             " VALUES (?,?,?,?,?,?,?,?,?)");


//	// NewOrder Txn
//	private PreparedStatement stmtGetCust = null;
//	private PreparedStatement stmtGetWhse = null;
//	private PreparedStatement stmtGetDist = null;
//	private PreparedStatement stmtInsertNewOrder = null;
//	private PreparedStatement stmtUpdateDist = null;
//	private PreparedStatement stmtInsertOOrder = null;
//	private PreparedStatement stmtGetItem = null;
//	private PreparedStatement stmtGetStock = null;
//	private PreparedStatement stmtUpdateStock = null;
//	private PreparedStatement stmtInsertOrderLine = null;


    public ResultSet run(Connection conn, Random gen,
			int terminalWarehouseID, int numWarehouses,
			int terminalDistrictLowerID, int terminalDistrictUpperID,
			TPCCWorker w) throws SQLException {



// 		//initializing all prepared statements
// 		stmtGetCust=this.getPreparedStatement(conn, stmtGetCustSQL);
// 		stmtGetWhse=this.getPreparedStatement(conn, stmtGetWhseSQL);
// 		stmtGetDist=this.getPreparedStatement(conn, stmtGetDistSQL);
// 		stmtInsertNewOrder=this.getPreparedStatement(conn, stmtInsertNewOrderSQL);
// 		stmtUpdateDist =this.getPreparedStatement(conn, stmtUpdateDistSQL);
// 		stmtInsertOOrder =this.getPreparedStatement(conn, stmtInsertOOrderSQL);
// 		stmtGetItem =this.getPreparedStatement(conn, stmtGetItemSQL);
// 		stmtGetStock =this.getPreparedStatement(conn, stmtGetStockSQL);
// 		stmtUpdateStock =this.getPreparedStatement(conn, stmtUpdateStockSQL);
// 		stmtInsertOrderLine =this.getPreparedStatement(conn, stmtInsertOrderLineSQL);


		int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID,terminalDistrictUpperID, gen);
		int customerID = TPCCUtil.getCustomerID(gen);

		int numItems = (int) TPCCUtil.randomNumber(5, 15, gen);
		int[] itemIDs = new int[numItems];
		int[] supplierWarehouseIDs = new int[numItems];
		int[] orderQuantities = new int[numItems];
		int allLocal = 1;
		for (int i = 0; i < numItems; i++) {
			itemIDs[i] = TPCCUtil.getItemID(gen);
			if (TPCCUtil.randomNumber(1, 100, gen) > 1) {
				supplierWarehouseIDs[i] = terminalWarehouseID;
			} else {
				do {
					supplierWarehouseIDs[i] = TPCCUtil.randomNumber(1,
							numWarehouses, gen);
				} while (supplierWarehouseIDs[i] == terminalWarehouseID
						&& numWarehouses > 1);
				allLocal = 0;
			}
			orderQuantities[i] = TPCCUtil.randomNumber(1, 10, gen);
		}

		// we need to cause 1% of the new orders to be rolled back.
		if (TPCCUtil.randomNumber(1, 100, gen) == 1)
			itemIDs[numItems - 1] = TPCCConfig.INVALID_ITEM_ID;


		newOrderTransaction(terminalWarehouseID, districtID,
						customerID, numItems, allLocal, itemIDs,
						supplierWarehouseIDs, orderQuantities, conn, w);
		return null;

    }


	private void newOrderTransaction(int w_id, int d_id, int c_id,
 			int o_ol_cnt, int o_all_local, int[] itemIDs,
 			int[] supplierWarehouseIDs, int[] orderQuantities, Connection conn, TPCCWorker w)
 			throws SQLException {
 		float c_discount, w_tax, d_tax = 0, i_price;
 		int d_next_o_id, o_id = -1, s_quantity;
 		String c_last = null, c_credit = null, i_name, i_data, s_data;
 		String s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05;
 		String s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, ol_dist_info = null;
 		float[] itemPrices = new float[o_ol_cnt];
 		float[] orderLineAmounts = new float[o_ol_cnt];
 		String[] itemNames = new String[o_ol_cnt];
 		int[] stockQuantities = new int[o_ol_cnt];
 		char[] brandGeneric = new char[o_ol_cnt];
 		int ol_supply_w_id, ol_i_id, ol_quantity;
 		int s_remote_cnt_increment;
 		float ol_amount, total_amount = 0;

 		String procedureString = "NewOrder Transaction: \n";
 		procedureString += "\t" +
				"SELECT C_DISCOUNT, C_LAST, C_CREDIT" +
				"  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
				" WHERE C_W_ID = " + Integer.toString(w_id) +
				"   AND C_D_ID = " + Integer.toString(d_id) +
				"   AND C_ID = " + Integer.toString(c_id) + "\n";
 		procedureString += "\t" +
				"SELECT W_TAX " +
				"  FROM " + TPCCConstants.TABLENAME_WAREHOUSE +
				" WHERE W_ID = " + Integer.toString(w_id) + "\n";
 		procedureString += "\t" +
				"SELECT D_NEXT_O_ID, D_TAX " +
				"  FROM " + TPCCConstants.TABLENAME_DISTRICT +
				" WHERE D_W_ID = " + Integer.toString(w_id) + " AND D_ID = " + Integer.toString(d_id) + " FOR UPDATE\n";
 		procedureString += "\t" +
				"UPDATE " + TPCCConstants.TABLENAME_DISTRICT +
				"   SET D_NEXT_O_ID = D_NEXT_O_ID + 1 " +
				" WHERE D_W_ID = " + Integer.toString(w_id) +
				"   AND D_ID = " + Integer.toString(d_id) + "\n";
 		procedureString += "\t" +
                "INSERT INTO " + TPCCConstants.TABLENAME_OPENORDER +
                " (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)" +
                " VALUES (?, " + Integer.toString(d_id) + ", " + Integer.toString(w_id) + ", " + Integer.toString(c_id) +
                ", " + w.getBenchmarkModule().getTimestamp(System.currentTimeMillis()).toString() + ", " + Integer.toString(o_ol_cnt) + ", " + Integer.toString(o_all_local) + ")\n";
 		procedureString += "\t" +
                "INSERT INTO " + TPCCConstants.TABLENAME_NEWORDER +
                " (NO_O_ID, NO_D_ID, NO_W_ID) " +
                " VALUES ( ?, " + Integer.toString(d_id) + ", " + Integer.toString(w_id) + ")\n";
 		String updateStockString = "";
 		String insertOrderLineString = "";
        for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
            ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
            ol_i_id = itemIDs[ol_number - 1];
            ol_quantity = orderQuantities[ol_number - 1];
            procedureString += "\t" +
                    "SELECT I_PRICE, I_NAME , I_DATA " +
                    "  FROM " + TPCCConstants.TABLENAME_ITEM +
                    " WHERE I_ID = " + Integer.toString(ol_i_id) + "\n";
            procedureString += "\t" +
                    "SELECT S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, " +
                    "       S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10" +
                    "  FROM " + TPCCConstants.TABLENAME_STOCK +
                    " WHERE S_I_ID = " + Integer.toString(ol_i_id) +
                    "   AND S_W_ID = " + Integer.toString(ol_supply_w_id) + " FOR UPDATE\n";
            updateStockString += "\t" +
                    "UPDATE " + TPCCConstants.TABLENAME_STOCK +
                    "   SET S_QUANTITY = ? , " +
                    "       S_YTD = S_YTD + " + Integer.toString(ol_quantity) + ", " +
                    "       S_ORDER_CNT = S_ORDER_CNT + 1, " +
                    "       S_REMOTE_CNT = S_REMOTE_CNT + ? " +
                    " WHERE S_I_ID = " + Integer.toString(ol_i_id) +
                    "   AND S_W_ID = " + Integer.toString(ol_supply_w_id) + "\n";
            insertOrderLineString += "\t" +
                    "INSERT INTO " + TPCCConstants.TABLENAME_ORDERLINE +
                    " (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) " +
                    " VALUES (?," + Integer.toString(d_id) + "," + Integer.toString(w_id) +
                    "," + Integer.toString(ol_number) + "," + Integer.toString(ol_i_id) + "," + Integer.toString(ol_supply_w_id) +
                    "," + Integer.toString(ol_quantity) + ",?,?)\n";
        }
        procedureString += insertOrderLineString + updateStockString;
        TPCCBenchmark.exec.log(procedureString);
    }

}
