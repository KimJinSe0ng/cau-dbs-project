import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.*;


public class DbConnection {
    private static final String URL = "jdbc:mysql://localhost:3306/codingtest";
    private static final String USER_NAME = "root";
    private static final String PASSWORD = "11111111";
    private static final String ADDRESS = "/Users/bigbird/Downloads/"; // csv파일을 어디다 저장할지 지정하는 주소

    private PreparedStatement preStatement;
    private Statement statement;
    private ResultSet result;
    private Connection con;

    private static final String[] LANGUAGES = {"Java", "Cpp", "Python", "JavaScript", "Kotlin", "Swift"};
    private static final String[] PARTS = {"BE", "FE", "AOS", "IOS"};
    private static final String[] PASS_FAIL = {"P", "F"};
    private static final String[] COLUMNS = {"language", "part", "pass_fl"};
    private static final int[] DOMAIN_LENGTH = {6, 4, 2};

    private static final int MAX_ROW_COUNT = 1000000;

    public void createTable() throws SQLException {
        String sql = "CREATE TABLE result ("
                + "reg_id INT NOT NULL AUTO_INCREMENT,"
                + "language VARCHAR(45),"
                + "part VARCHAR(45),"
                + "score INT,"
                + "pass_fl CHAR(1),"
                + "PRIMARY KEY (reg_id))";
        try {
            con = DriverManager.getConnection(URL, USER_NAME, PASSWORD);
            statement = con.createStatement();
            boolean tableExists = doesTableExist("result");
            if (tableExists) {
                System.out.println("테이블이 이미 생성되었습니다.");
                return;
            }
            statement.executeUpdate(sql);
            System.out.println("테이블 생성 완료");

            saveTableToCSV("/Users/bigbird/Downloads/result.csv", "result");
        } catch (SQLException e) {
            System.out.println("테이블 생성 과정에 오류 발생");
            e.printStackTrace();
        } finally {
            statement.close();
            con.close();
        }
    }


    public boolean doesTableExist(String tableName) throws SQLException {
        ResultSet resultSet = con.getMetaData().getTables(null, null, tableName, null);
        return resultSet.next();
    }

    public void saveTableToCSV(String fileName, String tableName) throws SQLException {
        try {
            con = DriverManager.getConnection(URL, USER_NAME, PASSWORD);
            statement = con.createStatement();

            String sql = "SELECT * FROM " + tableName;
            ResultSet resultSet = statement.executeQuery(sql);
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            FileWriter writer = new FileWriter(fileName);

            for (int i = 1; i <= columnCount; i++) {
                writer.append(metaData.getColumnName(i));
                if (i < columnCount) {
                    writer.append(",");
                }
            }
            writer.append("\n");

            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    writer.append(resultSet.getString(i));
                    if (i < columnCount) {
                        writer.append(",");
                    }
                }
                writer.append("\n");
            }

            writer.flush();
            writer.close();
            System.out.println("테이블 정보가 CSV 파일에 저장되었습니다.");

        } catch (SQLException | IOException e) {
            System.out.println("테이블 정보를 CSV 파일에 저장하는 과정에 오류 발생");
            e.printStackTrace();
        } finally {
            statement.close();
            con.close();
        }
    }

    public void insertRecords() throws SQLException {
        Random random = new Random();

        try {
            con = DriverManager.getConnection(URL, USER_NAME, PASSWORD);
            PreparedStatement preparedStatement = con.prepareStatement("INSERT INTO result (language, part, score, pass_fl) VALUES (?, ?, ?, ?)");

            boolean tableExists = doesTableExist("result");

            if (!tableExists) {
                System.out.println("테이블이 존재하지 않습니다. 레코드 삽입을 종료합니다.");
                return;
            }

            for (int i = 0; i < MAX_ROW_COUNT; i++) {
                String language = LANGUAGES[random.nextInt(LANGUAGES.length)];
                String part = PARTS[random.nextInt(PARTS.length)];
                int score = random.nextInt(401);
                String passFl = (score >= 300) ? PASS_FAIL[0] : PASS_FAIL[1];

                preparedStatement.setString(1, language);
                preparedStatement.setString(2, part);
                preparedStatement.setInt(3, score);
                preparedStatement.setString(4, passFl);

                preparedStatement.executeUpdate();
            }
            System.out.println("레코드 삽입 완료");

            saveTableToCSV(ADDRESS + "result.csv", "result");

        } catch (SQLException e) {
            System.out.println("레코드 삽입 과정에 오류 발생");
            e.printStackTrace();
        } finally {
            con.close();
        }
    }

    public static void CreateBitmapIndex() {
        try (Connection connection = DriverManager.getConnection(URL, USER_NAME, PASSWORD)) {
            for (int i = 0; i < COLUMNS.length; i++) { // 3까지 language, part, pass_fl
                String column = COLUMNS[i];
                int domainSize = DOMAIN_LENGTH[i]; // 6, 4, 2

                String query = "SELECT " + column + " FROM result";
                PreparedStatement statement = connection.prepareStatement(query);
                ResultSet resultSet = statement.executeQuery();

                StringBuilder[] bitString = new StringBuilder[domainSize]; // language가 6개 생성됨
                for (int j = 0; j < bitString.length; j++) {
                    bitString[j] = new StringBuilder(); // 초기화
                }

                while (resultSet.next()) { // 한 튜플 가져오는데 .getString(column)을 하는 column(eg.language)의 값을 value에 저장
                    String value = resultSet.getString(column); // value는 java, Cpp, python..
                    appendBitString(bitString, value, column); // language[5]가 넘어감, column: 현재 어떤 비트맵 컬럼인지 알려줌
                }

                try (FileWriter writer = new FileWriter(ADDRESS + column + "_bitmap_index.csv")) {

                    if (column.equals("language")) {
                        for (int s = 0; s < bitString.length; s++) { // 1번째 행에 헤더 정보(java, cpp..)
                            writer.write(LANGUAGES[s]);
                            if (s != bitString.length - 1) {
                                writer.write(",");
                            }
                        }
                    } else if (column.equals("part")) {
                        for (int s = 0; s < bitString.length; s++) { // 1번째 행에 헤더 정보(java, cpp..)
                            writer.write(PARTS[s]);
                            if (s != bitString.length - 1) {
                                writer.write(",");
                            }
                        }
                    } else if (column.equals("pass_fl")) {
                        for (int s = 0; s < bitString.length; s++) {
                            writer.write(PASS_FAIL[s]);
                            if (s != bitString.length - 1) {
                                writer.write(",");
                            }
                        }
                    }
                    writer.write("\n");
                    for (int k = 0; k < bitString.length; k++) { // bitString[0]:java의 010001011~
                        writer.write(bitString[k].toString());
                        if (k != bitString.length - 1) { // 마지막이 아니면 , 찍어서 구분해라
                            writer.write(",");
                        }
                    }
                    System.out.println(column + " Bitmap Index 정보가 CSV 파일에 저장되었습니다.");
                } catch (IOException e) {
                    e.printStackTrace();
                }

                resultSet.close();
                statement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void appendBitString(StringBuilder[] bitString, String value, String column) {
        if (column.equals("language")) {
            for (int i = 0; i < bitString.length; i++) { // bitString[0]:Java, [1]:Cpp
                int idx = Arrays.asList(LANGUAGES).indexOf(value);
                if (idx == i) {
                    bitString[i].append("1");
                } else {
                    bitString[i].append("0");
                }
            }
        } else if (column.equals("part")) {
            for (int i = 0; i < bitString.length; i++) { // bitString[0]:Java, [1]:Cpp
                int idx = Arrays.asList(PARTS).indexOf(value);
                if (idx == i) {
                    bitString[i].append("1");
                } else {
                    bitString[i].append("0");
                }
            }
        } else if (column.equals("pass_fl")) {
            for (int i = 0; i < bitString.length; i++) { // bitString[0]:Java, [1]:Cpp
                int idx = Arrays.asList(PASS_FAIL).indexOf(value);
                if (idx == i) {
                    bitString[i].append("1");
                } else {
                    bitString[i].append("0");
                }
            }
        }
    }

    private static final int BUFFER_SIZE = 80 * 1024; // 80KB
    private static final int BLOCK_SIZE = 16 * 1024; // 16KB

    public void BitmapQuerySearch() {
        int queryResult = 0;
        int locationIndex = 0;
        Byte[] arrResult = new Byte[MAX_ROW_COUNT];

        try {
            ByteBuffer[] bufferPages = new ByteBuffer[2];
            for (int page = 0; page < bufferPages.length; page++) {
                bufferPages[page] = ByteBuffer.allocate(BUFFER_SIZE);
            }

            ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
            BufferedReader languageReader = new BufferedReader(new FileReader(ADDRESS + "language_bitmap_index.csv"));
            BufferedReader passFLReader = new BufferedReader(new FileReader(ADDRESS + "pass_fl_bitmap_index.csv"));
            String line;

            passFLReader.readLine();
            languageReader.readLine();

            byte[] block_Java = new byte[BLOCK_SIZE];
            byte[] block_PASS = new byte[BLOCK_SIZE];
            byte[] result = new byte[BLOCK_SIZE];

            String JavaLine = languageReader.readLine();
            String JavaIndex = JavaLine.split(",")[0]; // 10101

            String PassLine = passFLReader.readLine();
            String PassIndex = PassLine.split(",")[0]; // 10010

            while (locationIndex < MAX_ROW_COUNT) {
                if (bufferPages[0].remaining() == 0 && bufferPages[1].remaining() == 0) {
                    bufferPages[0].clear();
                    bufferPages[1].clear();
                } else {
                    for (int i = 0; i < BLOCK_SIZE; i++) {
                        if (locationIndex < JavaIndex.length() && locationIndex < PassIndex.length()) {
                            char charJava = JavaIndex.charAt(locationIndex);
                            char charPASS = PassIndex.charAt(locationIndex);
                            block_Java[i] = (byte) (charJava == '1' ? 1 : 0);
                            block_PASS[i] = (byte) (charPASS == '1' ? 1 : 0);

                            result[i] = (byte) (block_Java[i] & block_PASS[i]);
                            arrResult[locationIndex] = result[i];
                            locationIndex += 1;
                        } else {
                            break;
                        }
                    }
                    bufferPages[0].put(block_Java);
                    bufferPages[1].put(block_PASS);
                }
            }
            con = con = DriverManager.getConnection(URL, USER_NAME, PASSWORD);
            String query = "SELECT reg_id FROM result";
            PreparedStatement statement = con.prepareStatement(query);
            ResultSet resultSet = statement.executeQuery();

            System.out.println("\n" +
                    ">> SELECT reg_id\n" +
                    ">> FROM result\n" +
                    ">> WHERE language =‘Java’ AND pass_fl =‘P’;");
            System.out.print(">> id: ");
            int index = 0;
            while (resultSet.next()) {
                int value = resultSet.getInt(1);
                if (arrResult[index] == 1) {
                    System.out.print(value + " ");
                }
                index += 1;
            }
            System.out.println();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void BitmapQueryAggregation() {
        int queryResult = 0;
        int locationIndex = 0;
        try {
            ByteBuffer[] bufferPages = new ByteBuffer[2];
            for (int page = 0; page < bufferPages.length; page++) {
                bufferPages[page] = ByteBuffer.allocate(BUFFER_SIZE);
            }

            ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
            BufferedReader partReader = new BufferedReader(new FileReader(ADDRESS + "part_bitmap_index.csv"));
            BufferedReader passFLReader = new BufferedReader(new FileReader(ADDRESS + "pass_fl_bitmap_index.csv"));
            String line;

            passFLReader.readLine();
            partReader.readLine();

            byte[] block_BE = new byte[BLOCK_SIZE];
            byte[] block_PASS = new byte[BLOCK_SIZE];
            byte[] result = new byte[BLOCK_SIZE];

            String BELine = partReader.readLine();
            String BEIndex = BELine.split(",")[0]; // 10101

            String PassLine = passFLReader.readLine();
            String PassIndex = PassLine.split(",")[0]; // 10010

            while (locationIndex < MAX_ROW_COUNT) {
                if (bufferPages[0].remaining() == 0 && bufferPages[1].remaining() == 0) {
                    bufferPages[0].clear();
                    bufferPages[1].clear();
                } else {
                    for (int i = 0; i < BLOCK_SIZE; i++) {
                        if (locationIndex < BEIndex.length() && locationIndex < PassIndex.length()) {
                            char charBE = BEIndex.charAt(locationIndex);
                            char charPASS = PassIndex.charAt(locationIndex);
                            block_BE[i] = (byte) (charBE == '1' ? 1 : 0);
                            block_PASS[i] = (byte) (charPASS == '1' ? 1 : 0);

                            result[i] = (byte) (block_BE[i] & block_PASS[i]);
                            if (result[i] == 1)
                                queryResult += 1;
                            locationIndex += 1;
                        } else {
                            break;
                        }
                    }
                    bufferPages[0].put(block_BE);
                    bufferPages[1].put(block_PASS);
                }
            }
            System.out.println("\n" +
                    ">> SELECT count(*)\n" +
                    ">> FROM result\n" +
                    ">> WHERE part =‘BE’ AND pass_fl =‘P’;");
            System.out.println(">> count: " + queryResult);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}

