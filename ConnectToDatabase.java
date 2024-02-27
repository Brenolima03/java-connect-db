import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException; 
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ConnectToDatabase {

    public static void main(String[] args) {
        // Declare envVariables to store environment variables.
        Map<String, String> envVariables = new HashMap<>();
        // Declare resultSetLines to store unique lines from the result set.
        Set<String> resultSetLines = new HashSet<>();
        
        // Read environment variables from .env file
        try (BufferedReader br = new BufferedReader(new FileReader(".env"))) {
            String line;
            // Read each line from the .env file
            while ((line = br.readLine()) != null) {
                // Split the line into key-value parts based on "=".
                String[] parts = line.split("=");
                if (parts.length == 2) {
                    // Trim and remove quotes from key and value,
                    // then store them in envVariables map.
                    String key = parts[0].trim();
                    String value = parts[1].trim().replaceAll("\"", "");
                    envVariables.put(key, value);
                }
            }
        } catch (IOException e) {
            // Print error message if an IOException occurs
            // while reading the .env file.
            System.out.println("Error reading .env file: " + e.getMessage());
        }

        // Get environment variables from the envVariables map.
        String host = envVariables.get("HOST");
        String database = envVariables.get("DATABASE");
        String user = envVariables.get("USER");
        String password = envVariables.get("PASSWORD");
        String client = envVariables.get("CLIENT");
        String port = envVariables.get("PORT");

        // Read SQL queries from queries.txt file and execute them.
        try (BufferedReader br = new BufferedReader(
            new FileReader("queries.txt")
        )) {
            // Concatenate all lines from queries.txt into a single string.
            String query = br.lines()
                .map(String::trim)
                .map(line -> line.replaceAll("\\\\n", "\n"))
                .collect(Collectors.joining(" "));

            try {
                // Establish database connection using JDBC driver.
                Class.forName(client + ".jdbc."
                + client.substring(0, 1).toUpperCase() + client.substring(1)
                + "Driver");
                String url = "jdbc:" + client + ":thin:@"
                + host + ":" + port + "/" + database;
                Connection connection = DriverManager.getConnection(
                    url,
                    user,
                    password
                );
                System.out.println("Connection successful!");

                // Execute the SQL query.
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(query);
                // Iterate over the result set and print unique lines.
                while (rs.next()) {
                    StringBuilder line = new StringBuilder();
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    for (int i = 1; i <= columnCount; i++) {
                        Object value = rs.getObject(i);
                        line.append(value);
                        // Append comma if it's not the last value.
                        if (i < columnCount) {
                            line.append(", ");
                        }
                    }
                    // Convert line to string.
                    String lineStr = line.toString();
                    // Check if the line is unique, then add to
                    // resultSetLines set and print.
                    if (!resultSetLines.contains(lineStr)) {
                        resultSetLines.add(lineStr);
                        System.out.println(lineStr);
                    }
                }

                // Close the database connection.
                connection.close();
            } catch (ClassNotFoundException | SQLException e) {
                // Print error message if a ClassNotFoundException
                // or SQLException occurs.
                System.out.println("Error executing query: " + e.getMessage());
            }
        } catch (IOException e) {
            // Print error message if an IOException occurs
            // while reading the queries.txt file.
            System.out.println(
                "Error reading queries.txt file: "+ e.getMessage()
            );
        }
    }

}
// java -cp lib/ojdbc11.jar ConnectToDatabase.java