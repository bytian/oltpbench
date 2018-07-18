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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

import com.oltpbenchmark.benchmarks.tpcc.*;
import org.apache.log4j.Logger;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;

public class Payment extends TPCCProcedure {

//    private static final Logger LOG = Logger.getLogger(Payment.class);
//
    public SQLStmt payUpdateWhseSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_WAREHOUSE +
            "   SET W_YTD = W_YTD + ? " +
            " WHERE W_ID = ? ");

    public SQLStmt payGetWhseSQL = new SQLStmt(
            "SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_NAME" +
            "  FROM " + TPCCConstants.TABLENAME_WAREHOUSE +
            " WHERE W_ID = ?");

    public SQLStmt payUpdateDistSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_DISTRICT +
            "   SET D_YTD = D_YTD + ? " +
            " WHERE D_W_ID = ? " +
            "   AND D_ID = ?");

    public SQLStmt payGetDistSQL = new SQLStmt(
            "SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_NAME" +
            "  FROM " + TPCCConstants.TABLENAME_DISTRICT +
            " WHERE D_W_ID = ? " +
            "   AND D_ID = ?");

    public SQLStmt payGetCustSQL = new SQLStmt(
            "SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, " +
            "       C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, " +
            "       C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " +
            "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_ID = ?");

    public SQLStmt payGetCustCdataSQL = new SQLStmt(
            "SELECT C_DATA " +
            "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_ID = ?");

    public SQLStmt payUpdateCustBalCdataSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER +
            "   SET C_BALANCE = ?, " +
            "       C_YTD_PAYMENT = ?, " +
            "       C_PAYMENT_CNT = ?, " +
            "       C_DATA = ? " +
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_ID = ?");

    public SQLStmt payUpdateCustBalSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER +
            "   SET C_BALANCE = ?, " +
            "       C_YTD_PAYMENT = ?, " +
            "       C_PAYMENT_CNT = ? " +
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_ID = ?");

    public SQLStmt payInsertHistSQL = new SQLStmt(
            "INSERT INTO " + TPCCConstants.TABLENAME_HISTORY +
            " (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) " +
            " VALUES (?,?,?,?,?,?,?,?)");

    public SQLStmt customerByNameSQL = new SQLStmt(
            "SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, " +
            "       C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, " +
            "       C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " +
            "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_LAST = ? " +
            " ORDER BY C_FIRST");

//    // Payment Txn
//    private PreparedStatement payUpdateWhse = null;
//    private PreparedStatement payGetWhse = null;
//    private PreparedStatement payUpdateDist = null;
//    private PreparedStatement payGetDist = null;
//    private PreparedStatement payGetCust = null;
//    private PreparedStatement payGetCustCdata = null;
//    private PreparedStatement payUpdateCustBalCdata = null;
//    private PreparedStatement payUpdateCustBal = null;
//    private PreparedStatement payInsertHist = null;
//    private PreparedStatement customerByName = null;

    public ResultSet run(Connection conn, Random gen,
                         int w_id, int numWarehouses,
                         int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) throws SQLException {

//        // initializing all prepared statements
//        payUpdateWhse = this.getPreparedStatement(conn, payUpdateWhseSQL);
//        payGetWhse = this.getPreparedStatement(conn, payGetWhseSQL);
//        payUpdateDist = this.getPreparedStatement(conn, payUpdateDistSQL);
//        payGetDist = this.getPreparedStatement(conn, payGetDistSQL);
//        payGetCust = this.getPreparedStatement(conn, payGetCustSQL);
//        payGetCustCdata = this.getPreparedStatement(conn, payGetCustCdataSQL);
//        payUpdateCustBalCdata = this.getPreparedStatement(conn, payUpdateCustBalCdataSQL);
//        payUpdateCustBal = this.getPreparedStatement(conn, payUpdateCustBalSQL);
//        payInsertHist = this.getPreparedStatement(conn, payInsertHistSQL);
//        customerByName = this.getPreparedStatement(conn, customerByNameSQL);

        // payUpdateWhse =this.getPreparedStatement(conn, payUpdateWhseSQL);

        String procedureString = "Payment Transaction: \n";

        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
        int customerID = TPCCUtil.getCustomerID(gen);

        int x = TPCCUtil.randomNumber(1, 100, gen);
        int customerDistrictID;
        int customerWarehouseID;
        if (x <= 85) {
            customerDistrictID = districtID;
            customerWarehouseID = w_id;
        } else {
            customerDistrictID = TPCCUtil.randomNumber(1, TPCCConfig.configDistPerWhse, gen);
            do {
                customerWarehouseID = TPCCUtil.randomNumber(1, numWarehouses, gen);
            } while (customerWarehouseID == w_id && numWarehouses > 1);
        }

        long y = TPCCUtil.randomNumber(1, 100, gen);
        boolean customerByName;
        String customerLastName = null;
        customerID = -1;
        if (y <= 60) {
            // 60% lookups by last name
            customerByName = true;
            customerLastName = TPCCUtil.getNonUniformRandomLastNameForRun(gen);
        } else {
            // 40% lookups by customer ID
            customerByName = false;
            customerID = TPCCUtil.getCustomerID(gen);
        }

        float paymentAmount = (float) (TPCCUtil.randomNumber(100, 500000, gen) / 100.0);

        String w_street_1, w_street_2, w_city, w_state, w_zip, w_name;
        String d_street_1, d_street_2, d_city, d_state, d_zip, d_name;

        procedureString += "\t" +
                "UPDATE " + TPCCConstants.TABLENAME_WAREHOUSE +
                "   SET W_YTD = W_YTD + " + Integer.toString(w_id) +
                " WHERE W_ID = " + Integer.toString(w_id) + "\n";

        procedureString += "\t" +
                "SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_NAME" +
                "  FROM " + TPCCConstants.TABLENAME_WAREHOUSE +
                " WHERE W_ID = " + Integer.toString(w_id) + "\n";

        procedureString += "\t" +
                "UPDATE " + TPCCConstants.TABLENAME_DISTRICT +
                "   SET D_YTD = D_YTD + " + Float.toString(paymentAmount) +
                " WHERE D_W_ID = " + Integer.toString(w_id) +
                "   AND D_ID = " + Integer.toString(districtID) + "\n";

        procedureString += "\t" +
                "SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_NAME" +
                "  FROM " + TPCCConstants.TABLENAME_DISTRICT +
                " WHERE D_W_ID = " + Integer.toString(w_id) +
                "   AND D_ID = " + Integer.toString(districtID) + "\n";

        if (customerByName) {
            procedureString += "\t" +
                    "SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, " +
                    "       C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, " +
                    "       C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " +
                    "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
                    " WHERE C_W_ID = " + Integer.toString(customerWarehouseID) +
                    "   AND C_D_ID = " + Integer.toString(customerDistrictID) +
                    "   AND C_LAST = " + customerLastName +
                    " ORDER BY C_FIRST\n";

        } else {
            procedureString += "\t" +
                    "SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, " +
                    "       C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, " +
                    "       C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " +
                    "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
                    " WHERE C_W_ID = " + Integer.toString(customerWarehouseID) +
                    "   AND C_D_ID = " + Integer.toString(customerDistrictID) +
                    "   AND C_ID = " + Integer.toString(customerID) + "\n";
        }

        long z = TPCCUtil.randomNumber(1, 100, gen);
        if (z <= 10) { // bad credit
            procedureString += "\t" +
                    "SELECT C_DATA " +
                    "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
                    " WHERE C_W_ID = " + Integer.toString(customerWarehouseID) +
                    "   AND C_D_ID = " + Integer.toString(customerDistrictID) +
                    "   AND C_ID = " + (customerByName ? "?" : Integer.toString(customerID)) + "\n";
            procedureString += "\t" +
                    "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER +
                    "   SET C_BALANCE = ?, " +
                    "       C_YTD_PAYMENT = ?, " +
                    "       C_PAYMENT_CNT = ?, " +
                    "       C_DATA = ? " +
                    " WHERE C_W_ID = " + Integer.toString(customerWarehouseID) +
                    "   AND C_D_ID = " + Integer.toString(customerDistrictID) +
                    "   AND C_ID = " + (customerByName ? "?" : Integer.toString(customerID)) + "\n";
        } else { // good credit
            procedureString += "\t" +
                    "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER +
                    "   SET C_BALANCE = ?, " +
                    "       C_YTD_PAYMENT = ?, " +
                    "       C_PAYMENT_CNT = ? " +
                    " WHERE C_W_ID = " + Integer.toString(customerWarehouseID) +
                    "   AND C_D_ID = " + Integer.toString(customerDistrictID) +
                    "   AND C_ID = " + (customerByName ? "?" : Integer.toString(customerID)) + "\n";
        }

        procedureString += "\t" +
                "INSERT INTO " + TPCCConstants.TABLENAME_HISTORY +
                " (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) " +
                " VALUES (" + Integer.toString(customerDistrictID) +
                "," + Integer.toString(customerWarehouseID) +
                "," + (customerByName ? "?" : Integer.toString(customerID)) +
                "," + Integer.toString(districtID) +
                "," + Integer.toString(w_id) +
                "," + w.getBenchmarkModule().getTimestamp(System.currentTimeMillis()).toString() +
                "," + Float.toString(paymentAmount) +
                ",?)\n";

        TPCCBenchmark.exec.log(procedureString);

        return null;
    }

//     // attention duplicated code across trans... ok for now to maintain separate
//     // prepared statements
//     public Customer getCustomerById(int c_w_id, int c_d_id, int c_id, Connection conn) throws SQLException {
//
//         payGetCust.setInt(1, c_w_id);
//         payGetCust.setInt(2, c_d_id);
//         payGetCust.setInt(3, c_id);
//         ResultSet rs = payGetCust.executeQuery();
//         if (!rs.next()) {
//             throw new RuntimeException("C_ID=" + c_id + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id + " not found!");
//         }
//
//         Customer c = TPCCUtil.newCustomerFromResults(rs);
//         c.c_id = c_id;
//         c.c_last = rs.getString("C_LAST");
//         rs.close();
//        return c;
//    }

//    // attention this code is repeated in other transacitons... ok for now to
//    // allow for separate statements.
//    public Customer getCustomerByName(int c_w_id, int c_d_id, String customerLastName) throws SQLException {
//        ArrayList<Customer> customers = new ArrayList<Customer>();
//
//        customerByName.setInt(1, c_w_id);
//        customerByName.setInt(2, c_d_id);
//        customerByName.setString(3, customerLastName);
//        ResultSet rs = customerByName.executeQuery();
//        if (LOG.isTraceEnabled()) LOG.trace("C_LAST=" + customerLastName + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id);
//
//        while (rs.next()) {
//            Customer c = TPCCUtil.newCustomerFromResults(rs);
//            c.c_id = rs.getInt("C_ID");
//            c.c_last = customerLastName;
//            customers.add(c);
//        }
//        rs.close();
//
//        if (customers.size() == 0) {
//            throw new RuntimeException("C_LAST=" + customerLastName + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id + " not found!");
//        }
//
//        // TPC-C 2.5.2.2: Position n / 2 rounded up to the next integer, but
//        // that
//        // counts starting from 1.
//        int index = customers.size() / 2;
//        if (customers.size() % 2 == 0) {
//            index -= 1;
//        }
//        return customers.get(index);
//    }


}
