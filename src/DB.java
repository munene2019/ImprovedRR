import java.io.*;
import java.sql.*;
import java.util.*;

public class  DB {
    private static final String DB_URL = "jdbc:mariadb://localhost:3306/rr";
    private static final String USER = "root";
    private static final String PASSWORD = "root";

    private static final String TASKS_CSV_FILE = "D:/Documents/Academics/KAGGLEDATA/tasks.csv";
    private static final String EMPLOYEES_CSV_FILE = "D:/Documents/Academics/KAGGLEDATA/tests.csv";

    public static void main(String[] args) {
        insertTasks();
        //insertWorkers();
    }
    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(DB_URL, USER, PASSWORD);
    }
    private static void insertTasks() {
        String insertQuery = "INSERT INTO tasks (priority, complexity, required_skill_level, name, category) VALUES (?, ?, ?, ?, ?)";
        int insertLimit = 20000; // maximum number of inserts
        int insertCount = 0;    // counter for inserted tasks

        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
             BufferedReader br = new BufferedReader(new FileReader(TASKS_CSV_FILE));
             PreparedStatement pstmt = conn.prepareStatement(insertQuery)) {

            conn.setAutoCommit(false);

            // Read header and map column indexes
            String headerLine = br.readLine();
            if (headerLine == null) {
                System.out.println("CSV file is empty!");
                return;
            }

            String[] headers = headerLine.split(",");
            Map<String, Integer> columnIndexMap = new HashMap<>();
            for (int i = 0; i < headers.length; i++) {
                columnIndexMap.put(headers[i].trim().toLowerCase(), i);
            }

            // Validate required columns
            String[] requiredColumns = {"taskname", "category", "skill", "complexity", "priority"};
            for (String col : requiredColumns) {
                if (!columnIndexMap.containsKey(col)) {
                    System.out.println("CSV file is missing required column: " + col);
                    return;
                }
            }

            String line;
            while ((line = br.readLine()) != null && insertCount < insertLimit) {
                String[] values = line.split(",");

                if (values.length < 5) {
                    System.out.println("Skipping invalid line (Incomplete Data): " + line);
                    continue;
                }

                try {
                    String taskName = values[columnIndexMap.get("taskname")].trim();
                    String category = values[columnIndexMap.get("category")].trim();
                    int skill = parseIntOrDefault(values, columnIndexMap.get("skill"), 1);
                    int complexity = parseIntOrDefault(values, columnIndexMap.get("complexity"), 1);
                    int priority = parseIntOrDefault(values, columnIndexMap.get("priority"), 1);

                    pstmt.setInt(1, priority);
                    pstmt.setInt(2, complexity);
                    pstmt.setInt(3, skill);
                    pstmt.setString(4, taskName);
                    pstmt.setString(5, category);
                    pstmt.executeUpdate();

                    insertCount++; // Increment after successful insert
                } catch (NumberFormatException e) {
                    System.out.println("Skipping invalid line (Invalid Number Format): " + line);
                }
            }

            conn.commit();
            System.out.println("CSV data inserted successfully! Inserted " + insertCount + " tasks.");
        } catch (IOException | SQLException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    //    private static void insertTasks() {
//        String insertQuery = "INSERT INTO tasks (priority, complexity, required_skill_level, name, category) VALUES (?, ?, ?, ?, ?)";
//
//        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
//             BufferedReader br = new BufferedReader(new FileReader(TASKS_CSV_FILE));
//             PreparedStatement pstmt = conn.prepareStatement(insertQuery)) {
//
//            conn.setAutoCommit(false);
//
//            // Read header and map column indexes
//            String headerLine = br.readLine();
//            if (headerLine == null) {
//                System.out.println("CSV file is empty!");
//                return;
//            }
//
//            String[] headers = headerLine.split(",");
//            Map<String, Integer> columnIndexMap = new HashMap<>();
//            for (int i = 0; i < headers.length; i++) {
//                columnIndexMap.put(headers[i].trim().toLowerCase(), i);
//            }
//
//            // Validate required columns
//            String[] requiredColumns = {"taskname", "category", "skill", "complexity", "priority"};
//            for (String col : requiredColumns) {
//                if (!columnIndexMap.containsKey(col)) {
//                    System.out.println("CSV file is missing required column: " + col);
//                    return;
//                }
//            }
//
//            String line;
//            while ((line = br.readLine()) != null) {
//                String[] values = line.split(",");
//
//                if (values.length < 5) {
//                    System.out.println("Skipping invalid line (Incomplete Data): " + line);
//                    continue;
//                }
//
//                try {
//                    String taskName = values[columnIndexMap.get("taskname")].trim();
//                    String category = values[columnIndexMap.get("category")].trim();
//                    int skill = parseIntOrDefault(values, columnIndexMap.get("skill"), 1);
//                    int complexity = parseIntOrDefault(values, columnIndexMap.get("complexity"), 1);
//                    int priority = parseIntOrDefault(values, columnIndexMap.get("priority"), 1);
//
//                    pstmt.setInt(1, priority);
//                    pstmt.setInt(2, complexity);
//                    pstmt.setInt(3, skill);
//                    pstmt.setString(4, taskName);
//                    pstmt.setString(5, category);
//                    pstmt.executeUpdate();
//
//                } catch (NumberFormatException e) {
//                    System.out.println("Skipping invalid line (Invalid Number Format): " + line);
//                }
//            }
//
//            conn.commit();
//            System.out.println("CSV data inserted successfully!");
//        } catch (IOException | SQLException e) {
//            System.err.println("Error: " + e.getMessage());
//            e.printStackTrace();
//        }
//    }
    private static void insertWorkers() {
        System.out.println("Starting worker insertion...");
        String insertQuery = "INSERT INTO workers (name, skill_level, load_limit, availability, category) VALUES (?, ?, ?, ?, ?)";

        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
             BufferedReader br = new BufferedReader(new FileReader(EMPLOYEES_CSV_FILE));
             PreparedStatement pstmt = conn.prepareStatement(insertQuery)) {

            conn.setAutoCommit(false);
            String headerLine = br.readLine();
            if (headerLine == null) {
                System.out.println("CSV file is empty!");
                return;
            }

            Map<String, Integer> columnIndexMap = mapColumnIndexes(headerLine);
            System.out.println("Column Index Map: " + columnIndexMap);

            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                if (values.length < 5) {
                    System.out.println("Skipping row (not enough values): " + Arrays.toString(values));
                    continue;
                }
                // Get column indexes correctly
                Integer firstNameIdx = columnIndexMap.get("First Name");
                Integer lastNameIdx = columnIndexMap.get("Last Name");
                Integer skillI = columnIndexMap.get("Skill"); // String
                Integer skillIdLevel = columnIndexMap.get("Skill Level"); // Integer
                Integer loadIdx = columnIndexMap.get("Load Limit");

                System.out.println("Column Index Mapping: " + columnIndexMap);

// Check if required columns exist
                if (firstNameIdx == null || lastNameIdx == null || skillIdLevel == null || loadIdx == null) {
                    System.out.println("Skipping row (missing required columns): " + Arrays.toString(values));
                    return;
                }

// Construct full name
                String firstName = values[firstNameIdx].trim();
                String lastName = values[lastNameIdx].trim();
                String fullName = firstName + " " + lastName;

// Retrieve and parse values correctly
                int skillLevel = parseIntOrDefault(values, skillIdLevel, 1); // Parse integer
                int loadLimit = parseIntOrDefault(values, loadIdx, 5);
                String skillCategory = values[skillI].trim(); // Keep as string

// Insert into database
                pstmt.setString(1, fullName);
                pstmt.setInt(2, skillLevel);
                pstmt.setInt(3, loadLimit);
                pstmt.setInt(4, 1); // Default availability
                pstmt.setString(5, skillCategory);

                pstmt.executeUpdate();

            }

            conn.commit();
            System.out.println("Workers inserted successfully!");
        } catch (IOException | SQLException e) {
            System.out.println("Workers insertion Error: " + e.getMessage());
        }
    }


    private static Map<String, Integer> mapColumnIndexes(String headerLine) {
        Map<String, Integer> columnIndexMap = new HashMap<>();
        String[] headers = headerLine.split(",");
        for (int i = 0; i < headers.length; i++) {
            columnIndexMap.put(headers[i].trim(), i);  // Keep original case
        }
        return columnIndexMap;
    }


    private static int parseIntOrDefault(String[] values, int index, int defaultValue) {
        try {
            if (index >= values.length || values[index].trim().isEmpty()) return defaultValue;
            return Integer.parseInt(values[index].trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
