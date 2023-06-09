import java.sql.SQLException;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) throws SQLException {
        DbConnection dbConnection = new DbConnection();
        Scanner scanner = new Scanner(System.in);
        int choice = 0;
        while (choice != 6) {
            System.out.println("--------------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("1. 테이블 생성 2. 레코드 삽입 3. Bitmap index 생성 4. Bitmap index를 이용한 multiple-key 질의처리 5. Bitmap index를 이용한 집계함수(*) 처리 6. 프로그램 종료 ");
            System.out.println("--------------------------------------------------------------------------------------------------------------------------------------");
            System.out.print(">> 입력: ");
            choice = scanner.nextInt();
            switch (choice) {
                case 1:
                    dbConnection.createTable();
                    break;
                case 2:
                    dbConnection.insertRecords();
                    break;
                case 3:
                    dbConnection.CreateBitmapIndex();
                    break;
                case 4:
                    dbConnection.BitmapQuerySearch();
                    break;
                case 5:
                    dbConnection.BitmapQueryAggregation();
                    break;
                case 6:
                    System.out.println("프로그램을 종료합니다.");
                    break;
                default:
                    System.out.println("유효하지 않은 선택입니다. 다시 선택해주세요.");
            }
        }

        scanner.close();
    }
}