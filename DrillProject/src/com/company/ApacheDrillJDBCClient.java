package com.company;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ApacheDrillJDBCClient {
    static final String JDBC_DRIVER = "com.mapr.drill.jdbc41.Driver";
    //static final String DB_URL = "jdbc:drill:zk=vm75-132.support.mapr.com:5181,vm75-133.support.mapr.com:5181/drill/aditya_cluster_com-drillbits";
    static final String DB_URL = "jdbc:drill:zk=vm75-167.support.mapr.com:5181,vm75-155.support.mapr.com:5181/drill/aditya-beta.cluster.com-drillbits";

    //using plain authentication

    static final String USER = "mapr";
    static final String PASS = "mapr";

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        try{
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(DB_URL,USER,PASS);

            stmt = conn.createStatement();
            /* Perform a select on data in the classpath storage plugin. */
            String sql = "select employee_id, first_name,last_name,salary FROM cp.`employee.json` " +
                    "where salary > 30000 and position_id=2";
            ResultSet rs = stmt.executeQuery(sql);
            System.out.println("EmployeeID" + " "+ "First Name " +"Last Name " + " "+ "Salary");
            System.out.println("-------------------------------------------------------------");
            while(rs.next()) {
                int employeeId  = rs.getInt("employee_id");
                String firstName = rs.getString("first_name");
                String lastName = rs.getString("last_name");
                String salary = rs.getString("salary");
                System.out.println(employeeId+ ":  "+ firstName+ " :  "+ lastName + " :  "+ salary);
            }

            rs.close();
            stmt.close();
            conn.close();
        } catch(SQLException se) {
            //Handle errors for JDBC
            se.printStackTrace();
        } catch(Exception e) {
            //Handle errors for Class.forName
            e.printStackTrace();
        } finally {
            try{
                if(stmt!=null)
                    stmt.close();
            } catch(SQLException se2) {
            }
            try {
                if(conn!=null)
                    conn.close();
            } catch(SQLException se) {
                se.printStackTrace();
            }
        }
    }
}