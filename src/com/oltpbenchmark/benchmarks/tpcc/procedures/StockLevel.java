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

import com.oltpbenchmark.benchmarks.tpcc.TPCCBenchmark;
import org.apache.log4j.Logger;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;

public class StockLevel extends TPCCProcedure {

//    private static final Logger LOG = Logger.getLogger(StockLevel.class);
//
	public SQLStmt stockGetDistOrderIdSQL = new SQLStmt(
	        "SELECT D_NEXT_O_ID " +
            "  FROM " + TPCCConstants.TABLENAME_DISTRICT +
	        " WHERE D_W_ID = ? " +
            "   AND D_ID = ?");

	public SQLStmt stockGetCountStockSQL = new SQLStmt(
	        "SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT " +
			" FROM " + TPCCConstants.TABLENAME_ORDERLINE + ", " + TPCCConstants.TABLENAME_STOCK +
			" WHERE OL_W_ID = ?" +
			" AND OL_D_ID = ?" +
			" AND OL_O_ID < ?" +
			" AND OL_O_ID >= ?" +
			" AND S_W_ID = ?" +
			" AND S_I_ID = OL_I_ID" +
			" AND S_QUANTITY < ?");

//	// Stock Level Txn
//	private PreparedStatement stockGetDistOrderId = null;
//	private PreparedStatement stockGetCountStock = null;

	 public ResultSet run(Connection conn, Random gen,
				int w_id, int numWarehouses,
				int terminalDistrictLowerID, int terminalDistrictUpperID,
				TPCCWorker w) throws SQLException {

//	     boolean trace = LOG.isTraceEnabled();
	     
//	     stockGetDistOrderId = this.getPreparedStatement(conn, stockGetDistOrderIdSQL);
//	     stockGetCountStock= this.getPreparedStatement(conn, stockGetCountStockSQL);

         String procedureString = "StockLevel Transaction: \n";

	     int threshold = TPCCUtil.randomNumber(10, 20, gen);
	     int d_id = TPCCUtil.randomNumber(terminalDistrictLowerID,terminalDistrictUpperID, gen);

	     int o_id = 0;
	     // XXX int i_id = 0;
	     int stock_count = 0;

	     procedureString += "\t" +
                 "SELECT D_NEXT_O_ID " +
                 "  FROM " + TPCCConstants.TABLENAME_DISTRICT +
                 " WHERE D_W_ID = " + Integer.toString(w_id) +
                 "   AND D_ID = " + Integer.toString(d_id) + "\n";

	     procedureString += "\t" +
                 "SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT " +
                 " FROM " + TPCCConstants.TABLENAME_ORDERLINE + ", " + TPCCConstants.TABLENAME_STOCK +
                 " WHERE OL_W_ID = " + Integer.toString(w_id) +
                 " AND OL_D_ID = " + Integer.toString(d_id) +
                 " AND OL_O_ID < ?" +
                 " AND OL_O_ID >= ?" +
                 " AND S_W_ID = " + Integer.toString(w_id) +
                 " AND S_I_ID = OL_I_ID" +
                 " AND S_QUANTITY < " + Integer.toString(threshold) + "\n";

         TPCCBenchmark.exec.log(procedureString);

         return null;
	 }
}
