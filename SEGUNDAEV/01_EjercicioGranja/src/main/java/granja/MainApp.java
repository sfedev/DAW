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

            CsvProcessorGranjeros csvProcessorGranjeros = new CsvProcessorGranjeros();
            CsvProcessorPlantaciones csvProcessorPlantaciones = new CsvProcessorPlantaciones();
            CsvProcessorRiegos csvProcessorRiegos = new CsvProcessorRiegos();
            CsvProcessorConstrucciones csvProcessorConstrucciones = new CsvProcessorConstrucciones();
            CsvProcessorTractores csvProcessorTractores = new CsvProcessorTractores();
            CsvProcessorGranjeroGranjero csvProcessorGranjeroGranjero = new CsvProcessorGranjeroGranjero();
            
            procesarCSVGranjero(conn, prop, "csv.granjeros.directory.path", csvProcessorGranjeros);
            Thread.sleep(2000);
            procesarCSVPlantaciones(conn, prop, "csv.plantaciones.directory.path", csvProcessorPlantaciones);
            Thread.sleep(2000);
            procesarCSVRiegos(conn, prop, "csv.riegos.directory.path", csvProcessorRiegos);
            Thread.sleep(2000);
            procesarCSVConstrucciones(conn, prop, "csv.construcciones.directory.path", csvProcessorConstrucciones);
            Thread.sleep(2000);
            procesarCSVTractores(conn, prop, "csv.tractores.directory.path", csvProcessorTractores);
            Thread.sleep(2000);
            procesarCSVGranjeroGranjero(conn, prop, "csv.granjero_granjero.directory.path", csvProcessorGranjeroGranjero);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    private static void procesarCSVGranjero(Connection conn, Properties prop, String propertyKey, CsvProcessorGranjeros processor) {
        String csvPath = prop.getProperty(propertyKey);
        File file = new File(csvPath);

        try {	
            processor.insertIntoGranjeros(conn, file);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void procesarCSVPlantaciones(Connection conn, Properties prop, String propertyKey, CsvProcessorPlantaciones processor) {
        String csvPath = prop.getProperty(propertyKey);
        File file = new File(csvPath);

        try {	
            processor.insertIntoPlantaciones(conn, file);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void procesarCSVRiegos(Connection conn, Properties prop, String propertyKey, CsvProcessorRiegos processor) {
        String csvPath = prop.getProperty(propertyKey);
        File file = new File(csvPath);

        try {	
            processor.insertIntoRiegos(conn, file);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void procesarCSVConstrucciones(Connection conn, Properties prop, String propertyKey, CsvProcessorConstrucciones processor) {
        String csvPath = prop.getProperty(propertyKey);
        File file = new File(csvPath);

        try {	
            processor.insertIntoConstrucciones(conn, file);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void procesarCSVTractores(Connection conn, Properties prop, String propertyKey, CsvProcessorTractores processor) {
        String csvPath = prop.getProperty(propertyKey);
        File file = new File(csvPath);

        try {	
            processor.insertIntoTractores(conn, file);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void procesarCSVGranjeroGranjero(Connection conn, Properties prop, String propertyKey, CsvProcessorGranjeroGranjero processor) {
        String csvPath = prop.getProperty(propertyKey);
        File file = new File(csvPath);

        try {	
            processor.insertIntoGranjeroGranjero(conn, file);
        } catch (Exception e) {
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
