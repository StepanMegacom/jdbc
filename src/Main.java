import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException, SQLException {

        Path path = Paths.get("src/test.csv");

        List<String> rows = Files.readAllLines(path);
        List<String[]> result = new ArrayList<>();
        int index = 0;
        long l = System.currentTimeMillis();
        for(int i = 0; i < rows.size(); i++){
            String s = rows.get(i);
            String s2 = s.replace(", ", " ");
            String [] strings = s2.split(",");
            result.add(strings);
            index++;
        }
        System.out.println(index);
        System.out.println((System.currentTimeMillis()-l)/1000 + " seconds");


        Connection con = DriverManager.getConnection(
                "jdbc:postgresql://localhost:5432/statistics2",
                "postgres", "postgres");
        con.setAutoCommit(false);

        try {

            PreparedStatement preparedStatement = con.prepareStatement("insert into " +
                    "public.statistics" +
                    "(id, Direction,Yearr,Datte,Weekday,Country,Commodity,Transport_Mode,Measure,Val,Cumulative) " +
                    "values (?,?,?,?,?,?,?,?,?,?,?);");

            Statement statement = con.createStatement();


            for (int i = 0; i < result.size(); i++) {
                preparedStatement.setLong(1, i);
                preparedStatement.setString(2, result.get(i)[0]);
                preparedStatement.setLong(3, Long.parseLong(result.get(i)[1]));
                preparedStatement.setString(4, result.get(i)[2]);
                preparedStatement.setString(5, result.get(i)[3]);
                preparedStatement.setString(6, result.get(i)[4]);
                preparedStatement.setString(7, result.get(i)[5]);
                preparedStatement.setString(8, result.get(i)[6]);
                preparedStatement.setString(9, result.get(i)[7]);
                preparedStatement.setLong(10, Long.parseLong(result.get(i)[8]));
                preparedStatement.setLong(11, Long.parseLong(result.get(i)[9]));

                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            con.commit();

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            System.out.println((System.currentTimeMillis()-l)/1000 + " seconds");

            con.close();
        }

    }
}
