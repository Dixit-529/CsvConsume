import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.beans.Statement;
import java.sql.SQLException;

import static java.lang.Integer.parseInt;

public class CsvConsume {
    public static void main(String[] args) {
        String jdbcUrl = "jdbc:mysql://localhost:3306/userinput";
        String userename = "root";
        String password ="Dixit@2002";

        String filepath = "C:\\Users\\VIKHYAT\\IdeaProjects\\CSVconsume\\src\\main\\resources\\demo.csv";

        try{
            Connection connection = DriverManager.getConnection(jdbcUrl, userename, password);
            connection.setAutoCommit(false);

            String sql = "insert into new_table(No,Name,Email,PhoneNumber) values(?,?,?,?)";

            try (PreparedStatement statement = connection.prepareStatement(sql)) {

                BufferedReader lineReader = new BufferedReader(new FileReader(filepath));

                String lineText = null;
                int count = 0;


                lineReader.readLine();

                int batchsize = 20;
                while ((lineText = lineReader.readLine()) != null) {
                    String[] data = lineText.split(",");

                    String No = data[0];
                    String Name = data[1];
                    String Eamil = data[2];
                    String PhoneNumber = data[3];

                    statement.setInt(1, parseInt(No));
                    statement.setString(2, Name);
                    statement.setString(3, Eamil);
                    statement.setDouble(4, parseInt(PhoneNumber));
                    statement.addBatch();
                    if ((count % batchsize) == 0) {
                        statement.executeBatch();

                    }
                }
                lineReader.close();
                statement.executeBatch();
            }
            connection.commit();
            connection.close();

            System.out.println("Data has been inserted successfully");

        }catch (Exception SQLException){
            System.out.println(SQLException);
        }
    }
}
