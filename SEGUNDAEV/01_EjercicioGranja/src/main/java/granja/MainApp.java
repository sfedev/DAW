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
    	
    	Properties prop = new Properties();
    	InputStream input = new FileInputStream("src/main/resources/application.properties");
		prop.load(input);
    	
        try {
            Connection conn = DatabaseManager.obtenerConexion();
            System.out.println("Conexión a la base de datos establecida");

            String csvGranjeros = prop.getProperty("csv.granjeros.directory.path");
            System.out.println("Ruta del directorio CSV: " + csvGranjeros);
            File file = new File(csvGranjeros);

            CsvProcessor.insertIntoGranjeros(conn, file);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
