import com.mysql.cj.conf.ConnectionUrl;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Executors;

public class CapitalizeServer {

    static String connectionUrl = "jdbc:mysql://localhost:3306";
    static String username = "Rasmus";
    static String password = "Class";
    static String database = "demodb";

    public static void main(String[] args) throws Exception {
        //Class.forName("com.mysql.jdbc.Driver");

        try (var listener = new ServerSocket(4444)) {
            System.out.println("The capitalization server is running...");
            var pool = Executors.newFixedThreadPool(20);
            while (true) {
                pool.execute(new Capitalizer(listener.accept()));
            }
        }
    }

    public static String updateTable(Connection con, String dbname, String whoToUpdate, String toUpdate) throws SQLException {
        PreparedStatement prepstmt = null;

        String query = "UPDATE " + dbname + ".name " + "SET names='" + toUpdate + "' " + "WHERE names='" + whoToUpdate + "'";


        try {
            prepstmt = con.prepareStatement(query);
            System.out.println("\n");
            System.out.println("Updated " + whoToUpdate + " With " + toUpdate + " in " + dbname);
            System.out.println("\n");
            prepstmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (prepstmt != null) {
                con.close();
            }
            return "Updated " + whoToUpdate + " With " + toUpdate + " in " + dbname;
        }
    }

    public static String removeFromTable(Connection con, String dbname, String toRemove) throws SQLException {
        PreparedStatement prepstmt = null;

        String query = "DELETE FROM " + dbname + ".name " + "WHERE names ='" + toRemove + "'";

        try {
            prepstmt = con.prepareStatement(query);
            System.out.println("\n");
            System.out.println("Removed " + toRemove + " From " + dbname);
            System.out.println("\n");
            prepstmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (prepstmt != null) {
                con.close();
            }
            return "Removed " + toRemove + " From " + dbname;
        }
    }

    public static String insertIntoTable(Connection con, String dbname, String input) throws SQLException {
        PreparedStatement prepstmt = null;

        String query = "INSERT INTO " + dbname + ".name " + "VALUES(?)";

        try {
            prepstmt = con.prepareStatement(query);
            prepstmt.setString(1, input);
            System.out.println("\n");
            System.out.println("Added " + input + " To " + dbname);
            System.out.println("\n");
            prepstmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (prepstmt != null) {
                con.close();
            }
            return "Added " + input + " To " + dbname;
        }
    }

    public static List<String> ViewTable(Connection con, String dbname) throws SQLException {

        PreparedStatement stmt = null;
        String query = "select *" + "from " + dbname + ".name";


        ArrayList<String> names = new ArrayList<>();

        try {
            stmt = con.prepareStatement(query);
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                String name = rs.getString("names");
                names.add(name);
            }
        } catch (SQLException e) {
            System.out.println(e);
        } finally {
            if (stmt != null) {
                stmt.close();
            }
            return names;
        }
    }

    private static class Capitalizer implements Runnable {
        private Socket socket;

        Capitalizer(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            System.out.println("Connected: " + socket);
            try {
                var in = new Scanner(socket.getInputStream());
                var out = new PrintWriter(socket.getOutputStream(), true);

                while (in.hasNextLine()) {

                    switch (in.nextLine()) {
                        case "Retrieve":
                            out.println("Current names in the Database: ");
                            out.println("\n");
                            out.println(ViewTable(DriverManager.getConnection
                                    (connectionUrl, username, password), database));
                            break;
                        case "Create":
                            out.println(insertIntoTable(DriverManager.getConnection
                                    (connectionUrl, username, password), database , in.nextLine()));
                            break;
                        case "Update":
                            out.println(updateTable(DriverManager.getConnection
                                    (connectionUrl, username, password), database , in.nextLine(),in.nextLine()));
                            break;
                        case "Delete":
                            out.println(removeFromTable(DriverManager.getConnection
                                    (connectionUrl, username, password), database , in.nextLine()));
                            break;
                        default:
                            out.println("Unknown Command");
                            break;
                    }
                }
            } catch (Exception e) {
                System.out.println("Error:" + socket);
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                }
                System.out.println("Closed: " + socket);
            }
        }
    }
}