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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

import com.oltpbenchmark.benchmarks.tpcc.TPCCBenchmark;
import org.apache.log4j.Logger;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;

public class OrderStatus extends TPCCProcedure {

//    private static final Logger LOG = Logger.getLogger(OrderStatus.class);
//
	public SQLStmt ordStatGetNewestOrdSQL = new SQLStmt(
	        "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D " +
            "  FROM " + TPCCConstants.TABLENAME_OPENORDER +
            " WHERE O_W_ID = ? " +
            "   AND O_D_ID = ? " +
            "   AND O_C_ID = ? " +
            " ORDER BY O_ID DESC LIMIT 1");

	public SQLStmt ordStatGetOrderLinesSQL = new SQLStmt(
	        "SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D " +
            "  FROM " + TPCCConstants.TABLENAME_ORDERLINE +
            " WHERE OL_O_ID = ?" +
            "   AND OL_D_ID = ?" +
            "   AND OL_W_ID = ?");

	public SQLStmt payGetCustSQL = new SQLStmt(
	        "SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, " +
            "       C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, " +
            "       C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " +
            "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_ID = ?");

	public SQLStmt customerByNameSQL = new SQLStmt(
	        "SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, " +
            "       C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, " +
            "       C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " +
            "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_LAST = ? " +
            " ORDER BY C_FIRST");

//	private PreparedStatement ordStatGetNewestOrd = null;
//	private PreparedStatement ordStatGetOrderLines = null;
//	private PreparedStatement payGetCust = null;
//	private PreparedStatement customerByName = null;


    public ResultSet run(Connection conn, Random gen, int w_id, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) throws SQLException {
//        boolean trace = LOG.isTraceEnabled();
        
//        // initializing all prepared statements
//        payGetCust = this.getPreparedStatement(conn, payGetCustSQL);
//        customerByName = this.getPreparedStatement(conn, customerByNameSQL);
//        ordStatGetNewestOrd = this.getPreparedStatement(conn, ordStatGetNewestOrdSQL);
//        ordStatGetOrderLines = this.getPreparedStatement(conn, ordStatGetOrderLinesSQL);

        int d_id = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
        boolean c_by_name = false;
        int y = TPCCUtil.randomNumber(1, 100, gen);
        String c_last = null;
        int c_id = -1;
        if (y <= 60) {
            c_by_name = true;
            c_last = TPCCUtil.getNonUniformRandomLastNameForRun(gen);
        } else {
            c_by_name = false;
            c_id = TPCCUtil.getCustomerID(gen);
        }

        int o_id = -1, o_carrier_id = -1;
        Timestamp o_entry_d;
        ArrayList<String> orderLines = new ArrayList<String>();


        String procedureString = "OrderStatus Transaction: \n";

        Customer c;
        if (c_by_name) {
            procedureString += "\t" +
                    "SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, " +
                    "       C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, " +
                    "       C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " +
                    "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
                    " WHERE C_W_ID = " + Integer.toString(w_id) +
                    "   AND C_D_ID = " + Integer.toString(d_id) +
                    "   AND C_LAST = " + c_last +
                    " ORDER BY C_FIRST\n";
        } else {
            procedureString += "\t" +
                    "SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, " +
                    "       C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, " +
                    "       C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " +
                    "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
                    " WHERE C_W_ID = " + Integer.toString(w_id) +
                    "   AND C_D_ID = " + Integer.toString(d_id) +
                    "   AND C_ID = " + Integer.toString(c_id) + "\n";
        }

        // find the newest order for the customer
        // retrieve the carrier & order date for the most recent order.

        String c_id_string = c_by_name ? "?" : Integer.toString(c_id);

        procedureString += "\t" +
                "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D " +
                "  FROM " + TPCCConstants.TABLENAME_OPENORDER +
                " WHERE O_W_ID = " + Integer.toString(w_id) +
                "   AND O_D_ID = " + Integer.toString(d_id) +
                "   AND O_C_ID = " + c_id_string +
                " ORDER BY O_ID DESC LIMIT 1\n";

        procedureString += "\t" +
                "SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D " +
                "  FROM " + TPCCConstants.TABLENAME_ORDERLINE +
                " WHERE OL_O_ID = ?" +
                "   AND OL_D_ID = " + Integer.toString(d_id) +
                "   AND OL_W_ID = " + Integer.toString(w_id) + "\n";

        TPCCBenchmark.exec.log(procedureString);

        return null;
    }

//    // attention duplicated code across trans... ok for now to maintain separate
//    // prepared statements
//    public Customer getCustomerById(int c_w_id, int c_d_id, int c_id, Connection conn) throws SQLException {
//        boolean trace = LOG.isTraceEnabled();
//
//        payGetCust.setInt(1, c_w_id);
//        payGetCust.setInt(2, c_d_id);
//        payGetCust.setInt(3, c_id);
//        if (trace) LOG.trace("payGetCust START");
//        ResultSet rs = payGetCust.executeQuery();
//        if (trace) LOG.trace("payGetCust END");
//        if (!rs.next()) {
//            String msg = String.format("Failed to get CUSTOMER [C_W_ID=%d, C_D_ID=%d, C_ID=%d]",
//                                       c_w_id, c_d_id, c_id);
//            if (trace) LOG.warn(msg);
//            throw new RuntimeException(msg);
//        }
//
//        Customer c = TPCCUtil.newCustomerFromResults(rs);
//        c.c_id = c_id;
//        c.c_last = rs.getString("C_LAST");
//        rs.close();
//        return c;
//    }
//
//    // attention this code is repeated in other transacitons... ok for now to
//    // allow for separate statements.
//    public Customer getCustomerByName(int c_w_id, int c_d_id, String c_last) throws SQLException {
//        ArrayList<Customer> customers = new ArrayList<Customer>();
//        boolean trace = LOG.isDebugEnabled();
//
//        customerByName.setInt(1, c_w_id);
//        customerByName.setInt(2, c_d_id);
//        customerByName.setString(3, c_last);
//        if (trace) LOG.trace("customerByName START");
//        ResultSet rs = customerByName.executeQuery();
//        if (trace) LOG.trace("customerByName END");
//
//        while (rs.next()) {
//            Customer c = TPCCUtil.newCustomerFromResults(rs);
//            c.c_id = rs.getInt("C_ID");
//            c.c_last = c_last;
//            customers.add(c);
//        }
//        rs.close();
//
//        if (customers.size() == 0) {
//            String msg = String.format("Failed to get CUSTOMER [C_W_ID=%d, C_D_ID=%d, C_LAST=%s]",
//                                       c_w_id, c_d_id, c_last);
//            if (trace) LOG.warn(msg);
//            throw new RuntimeException(msg);
//        }
//
//        // TPC-C 2.5.2.2: Position n / 2 rounded up to the next integer, but
//        // that counts starting from 1.
//        int index = customers.size() / 2;
//        if (customers.size() % 2 == 0) {
//            index -= 1;
//        }
//        return customers.get(index);
//    }



}



