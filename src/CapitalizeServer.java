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

    static String connectionUrl = "jdbc:sqlserver://localhost;database=NordicWaterUniverseDB";
    static String username = "sa";
    static String password = "1234";
    static String database = "NordicWaterUniverseDB";

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

        String query = "UPDATE " + dbname + ".dbo.CheckedIn " + "SET ChipID='" + toUpdate + "' " + "WHERE CheckedIn='" + whoToUpdate + "'";


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

        String query = "DELETE FROM " + dbname + ".dbo.CheckedIn " + "WHERE ChipID ='" + toRemove + "'";

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

        String query = "INSERT INTO " + dbname + ".dbo.CheckedIn " + "VALUES(?)";

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

    public static List<String> ViewTable(Connection con, String dbname, String area) throws SQLException {

        PreparedStatement stmt = null;
        String query = "select *" + " from " + dbname + ".dbo.CheckedIn" + " WHERE " + dbname + ".dbo.CheckedIn.Area = " + area;


        ArrayList<String> ids = new ArrayList<>();

        try {
            stmt = con.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String id = rs.getString("ChipID");
                ids.add(id);
                id = rs.getString("CheckInTime");
                ids.add(id);
                ids.add("\n");
            }
        } catch (SQLException e) {
            System.out.println(e);
        } finally {
            if (stmt != null) {
                stmt.close();
            }
            if (ids.isEmpty()){
                ids.add("No one is Checked In");
            }
            System.out.println(ids);
            return ids;
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
                        case "Retrieve Area 1":
                            out.println("Current Customers Checked In in Area 1: ");
                            out.println(ViewTable(DriverManager.getConnection
                                    (connectionUrl, username, password), database, in.nextLine()));
                            out.println();
                            break;
                        case "Retrieve Area 2":
                            out.println("Current Customers Checked In in Area 2");
                            out.println(ViewTable(DriverManager.getConnection
                                    (connectionUrl, username, password), database, in.nextLine()));
                            out.println();
                            break;
                        case "Retrieve Area 3":
                            out.println("Current Customers Checked In in Area 3");
                            out.println(ViewTable(DriverManager.getConnection
                                    (connectionUrl, username, password), database, in.nextLine()));
                            out.println();
                            break;
                        case "Retrieve Area 4":
                            out.println("Current Customers Checked In in Area 4");
                            out.println(ViewTable(DriverManager.getConnection
                                    (connectionUrl, username, password), database, in.nextLine()));
                            out.println();
                            break;
                        /*case "Create":
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
                            break;*/
                        default:
                            //out.println("Unknown Command");
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