package granja;

import com.opencsv.CSVReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

public class EjercicioGranja {

	public static void main(String[] args) throws IOException {
		Properties prop = new Properties();
        InputStream input = new FileInputStream("src/main/resources/application.properties");
        prop.load(input);

        String dbUrl = prop.getProperty("db.url");
        String dbUser = prop.getProperty("db.user");
        String dbPassword = prop.getProperty("db.password");
        String csvDirectoryPath = prop.getProperty("csv.directory.path");

        File folder = new File(csvDirectoryPath);
        File[] listOfFiles = folder.listFiles();
        
		/*
		for (File file : listOfFiles) {
			System.out.println(file);
			try (BufferedReader br = new BufferedReader(new FileReader(file))) {
				String line;
				while ((line = br.readLine()) != null) {
					System.out.println(line);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		*/


        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
            for (File file : listOfFiles) {
                if (file.isFile() && file.getName().endsWith(".csv")) {
                    switch (file.getName()) {
	                    case "construcciones.csv":
	                    	System.out.println(file);
	                    	//insertIntoConstrucciones(conn, file);
	                        break;
	                    case "granjeros.csv":
	                    	System.out.println(file);
	                    	insertIntoGranjeros(conn, file);
	                        break;
	                    case "granjero_granjero.csv":
	                    	System.out.println(file);
	                    	//insertIntoGranjeroGranjero(conn, file);
	                        break;
	                    case "plantaciones.csv":
	                    	System.out.println(file);
	                    	//insertIntoPlantaciones(conn, file);
	                        break;
	                    case "riegos.csv":
	                    	System.out.println(file);
	                    	//insertIntoRiegos(conn, file);
	                        break;
	                        
	                    case "tractores.csv":
	                    	System.out.println(file);
	                    	//insertIntoTractores(conn, file);
	                        break;
                        default:
                            System.out.println("Archivo desconocido: " + file.getName());
                            break;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
	
	
	private static void insertIntoGranjeros(Connection conn, File file) throws Exception {
        String insertQuery = "INSERT INTO granjeros(id, nombre, descripcion, dinero, puntos, nivel) VALUES (?, ?, ?, ?, ?, ?)";
        insertIntoTable(conn, file, insertQuery);
    }
	
	/*
	private static void insertIntoConstrucciones(Connection conn, File file) throws Exception {
		String insertQuery = "INSERT INTO construcciones(id, nombre, precio, id_granjero) VALUES (?, ?, ?, ?)";
		insertIntoTable(conn, file, insertQuery);
	}
	
	private static void insertIntoGranjeroGranjero(Connection conn, File file) throws Exception {
        String insertQuery = "INSERT INTO granjero_granjero(id_granjero, id_vecino, puntos_compartidos) VALUES (?, ?, ?)";
        insertIntoTable(conn, file, insertQuery);
    }
    
    private static void insertIntoPlantaciones(Connection conn, File file) throws Exception {
        String insertQuery = "INSERT INTO plantaciones(id, nombre, precio_compra, precio_venta, proxima_cosecha, id_granjero) VALUES (?, ?, ?, ?, ?, ?)";
        insertIntoTable(conn, file, insertQuery);
    }
    
    private static void insertIntoRiegos(Connection conn, File file) throws Exception {
        String insertQuery = "INSERT INTO riegos(id, tipo, velocidad, plantacion) VALUES (?, ?, ?, ?)";
        insertIntoTable(conn, file, insertQuery);
    }
    
    private static void insertIntoTractores(Connection conn, File file) throws Exception {
        String insertQuery = "INSERT INTO tractores(id, modelo, velocidad, precio_venta, id_construccion) VALUES (?, ?, ?, ?, ?)";
        insertIntoTable(conn, file, insertQuery);
    }
	*/
    

    private static void insertIntoTable(Connection conn, File file, String insertQuery) throws Exception {
        CSVReader reader = new CSVReader(new FileReader(file));
        PreparedStatement pstmt = conn.prepareStatement(insertQuery);
        String[] rowData = null;
        while((rowData = reader.readNext()) != null) {
        	try {
        	    for (int i = 0; i < rowData.length; i++) {
        	        pstmt.setString(i + 1, rowData[i]);
        	    }
        	    pstmt.addBatch();
        	} catch (SQLException e) {
        	    if (e.getErrorCode() == 1062) {
        	        logDuplicate(rowData);
        	    } else {
        	        logError(e);
        	    }
        	}
        }

        int[] insertCounts = pstmt.executeBatch();
        System.out.println("Se han insertado " + insertCounts.length + " filas en la base de datos desde el archivo: " + file.getName());
	}
    
    private static void logDuplicate(String[] rowData) throws IOException {
    	Properties prop = new Properties();
        InputStream input = new FileInputStream("src/main/resources/application.properties");
        prop.load(input);
    	String duplicados = prop.getProperty("duplicates.file.path");
        File duplicatesFile = new File(duplicados);
        if (!duplicatesFile.exists()) {
            duplicatesFile.getParentFile().mkdirs();
            duplicatesFile.createNewFile();
        }
        FileWriter fileWriter = new FileWriter(duplicatesFile, true); // true para append
        PrintWriter printWriter = new PrintWriter(fileWriter);
        printWriter.println(Arrays.toString(rowData)); // Imprime el registro duplicado
        printWriter.close();
    }

    private static void logError(Exception e) throws IOException {
    	Properties prop = new Properties();
        InputStream input = new FileInputStream("src/main/resources/application.properties");
        prop.load(input);
        String errores = prop.getProperty("errors.file.path");
        File errorsFile = new File(errores);
        if (!errorsFile.exists()) {
            errorsFile.getParentFile().mkdirs();
            errorsFile.createNewFile();
        }
        FileWriter fileWriter = new FileWriter(errorsFile, true); // true para append
        PrintWriter printWriter = new PrintWriter(fileWriter);
        printWriter.println(e.getMessage()); // Imprime el mensaje de error
        printWriter.close();
    }

}
