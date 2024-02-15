package granja;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CsvProcessor {

    public static void insertIntoGranjeros(Connection conn, File directory) {
        String insertQuery = "INSERT INTO Granjeros(id, nombre, descripcion, dinero, puntos, nivel) VALUES (?, ?, ?, ?, ?, ?)";

        if (directory.isDirectory()) {
            File[] files = directory.listFiles();

            if (files != null) {
                for (File file : files) {
                    if (file.isFile() && file.getName().endsWith(".csv")) {
                        processCsvFile(conn, file, insertQuery);
                    }
                }
            }
        }
    }

    private static void processCsvFile(Connection conn, File file, String insertQuery) {
        try (BufferedReader br = Files.newBufferedReader(file.toPath())) {
            String line;
            boolean firstLine = true;

            while ((line = br.readLine()) != null) {
                if (firstLine) {
                    firstLine = false;
                    continue;
                }

                String[] rowData = line.split(",");
                try (PreparedStatement preparedStatement = conn.prepareStatement(insertQuery)) {
                    preparedStatement.setInt(1, Integer.parseInt(rowData[0]));
                    preparedStatement.setString(2, rowData[1]);
                    preparedStatement.setString(3, rowData[2]);
                    preparedStatement.setFloat(4, 0);
                    preparedStatement.setInt(5, Integer.parseInt(rowData[4]));
                    preparedStatement.setInt(6, Integer.parseInt(rowData[5]));

                    preparedStatement.executeUpdate();
                    System.out.println("Inserción exitosa en la base de datos");

                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
