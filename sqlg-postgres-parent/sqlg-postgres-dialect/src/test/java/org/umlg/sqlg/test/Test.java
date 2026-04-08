package org.umlg.sqlg.test;

import java.sql.*;

public class Test {

    public static void main(String[] args) {
        String url = "jdbc:postgresql://10.70.41.155:9999/cm";
        String user = "cm";
        String password = "cm";

        for (int i = 0; i < 10; i++) {
            try (Connection conn = DriverManager.getConnection(url, user, password)) {
//                conn
//                System.out.println("Connected to PostgreSQL!");
//                // Example query
//                Statement stmt = conn.createStatement();
//                ResultSet rs = stmt.executeQuery("SELECT * from \"V_Group\"");
//                while (rs.next()) {
//                    System.out.println(rs.getString(1));
//                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
