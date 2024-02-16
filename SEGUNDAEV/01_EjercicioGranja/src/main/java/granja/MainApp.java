package granja;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class MainApp {

    public static void main(String[] args) throws IOException {
        Properties prop = loadProperties();

        try (Connection conn = DatabaseManager.obtenerConexion()) {
            System.out.println("Conexión a la base de datos establecida");

            String csvGranjeros = prop.getProperty("csv.granjeros.directory.path");
            File file = new File(csvGranjeros);

            try {
                CsvProcessorGranjeros.insertIntoGranjeros(conn, file);
            } catch (Exception e) {
                e.printStackTrace();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static Properties loadProperties() throws IOException {
        Properties prop = new Properties();
        try (InputStream input = new FileInputStream("src/main/resources/application.properties")) {
            prop.load(input);
        }
        return prop;
    }
}

