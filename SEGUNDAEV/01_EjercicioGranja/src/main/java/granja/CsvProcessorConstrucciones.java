package granja;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class CsvProcessorConstrucciones {

	public static void insertIntoConstrucciones(Connection conn, File directoryPath) {
	    String insertQuery = "INSERT INTO Construcciones(id, nombre, precio, id_granjero) VALUES (?, ?, ?, ?)";
	    List<String[]> dbRecords = new ArrayList<>();
	    List<String[]> csvRecords = new ArrayList<>();
	    Set<String> seenIds = new HashSet<>();

	    if (directoryPath.isDirectory()) {
	        File[] files = directoryPath.listFiles();

	        if (files != null) {
	            for (File file : files) {
	                if (file.isFile() && file.getName().endsWith(".csv")) {
	                    try (BufferedReader br = Files.newBufferedReader(file.toPath())) {
	                        String line;
	                        boolean firstLine = true;

	                        while ((line = br.readLine()) != null) {
	                            if (firstLine) {
	                                firstLine = false;
	                                continue;
	                            }

	                            String[] rowData = line.split(",");
	                            if (seenIds.contains(rowData[0])) {
	                                logDuplicate(rowData);
	                            } else {
	                                seenIds.add(rowData[0]);
	                            }
	                            csvRecords.add(rowData);

	                            // Realiza la inserción en la base de datos utilizando conn y rowData
	                            try (PreparedStatement preparedStatement = conn.prepareStatement(insertQuery)) {
	                                preparedStatement.setInt(1, Integer.parseInt(rowData[0]));
	                                preparedStatement.setString(2, rowData[1]);
	                                preparedStatement.setFloat(3, Float.parseFloat(rowData[2]));

	                                // Manejo de valores nulos o vacíos para id_granjero
	                                preparedStatement.setNull(4, java.sql.Types.INTEGER);
	                                if (rowData.length >= 4 && !rowData[3].isEmpty()) {
	                                    preparedStatement.setInt(4, Integer.parseInt(rowData[3]));
	                                }

	                                preparedStatement.executeUpdate();

	                                // Agrega el registro a la lista de registros insertados en la base de datos
	                                dbRecords.add(rowData);
	                                System.out.println("Inserción exitosa en la base de datos");

	                            } catch (SQLException e) {
	                                handleSQLException(e);
	                            }
	                        }
	                    } catch (IOException e) {
	                        handleIOException(e);
	                    }
	                }
	            }
	        }

	        // Ahora, compara y realiza las acciones necesarias
	        compareAndProcessRecords(conn, dbRecords, csvRecords);
	    }
	}


    private static void compareAndProcessRecords(Connection conn, List<String[]> dbRecords, List<String[]> csvRecords) {
        Set<String> duplicateIds = new HashSet<>();

        for (String[] csvRecord : csvRecords) {
            for (String[] dbRecord : dbRecords) {
                if (csvRecord[0].equals(dbRecord[0]) && areRecordsDifferent(csvRecord, dbRecord)) {
                    // Si el primer valor (id) es igual y hay al menos un valor diferente, realiza las acciones necesarias
                    updateRecordInDatabase(conn, csvRecord);
                    duplicateIds.add(csvRecord[0]);
                }
            }
        }

        // Registra los registros duplicados en el archivo duplicados.log
        for (String[] csvRecord : csvRecords) {
            if (duplicateIds.contains(csvRecord[0])) {
                logDuplicate(csvRecord);
            }
        }
    }

    private static boolean areRecordsDifferent(String[] record1, String[] record2) {
        // Compara los valores de los registros, excluyendo el primer valor (id)
        for (int i = 1; i < record1.length; i++) {
            if (!record1[i].equals(record2[i])) {
                return true; // Hay al menos un valor diferente
            }
        }
        return false; // Todos los valores son iguales
    }

    private static void updateRecordInDatabase(Connection conn, String[] newValues) {
        String updateQuery = "UPDATE Construcciones SET nombre=?, precio=?, id_granjero=? WHERE id=?";

        try (PreparedStatement preparedStatement = conn.prepareStatement(updateQuery)) {
            // Configura los parámetros del PreparedStatement con los nuevos valores
            preparedStatement.setString(1, newValues[1]);
            preparedStatement.setFloat(2, Float.parseFloat(newValues[2]));
            preparedStatement.setInt(3, Integer.parseInt(newValues[3]));
            preparedStatement.setInt(4, Integer.parseInt(newValues[0]));

            // Ejecuta la actualización en la base de datos
            preparedStatement.executeUpdate();
            System.out.println("Actualización exitosa en la base de datos");

        } catch (SQLException e) {
            handleSQLException(e);
        }
    }

    private static void logDuplicate(String[] rowData) {
        Properties prop = new Properties();
        InputStream input = null;

        try {
            input = new FileInputStream("src/main/resources/application.properties");
            prop.load(input);
            String duplicados = prop.getProperty("duplicates.file.path");
            Path duplicatesFilePath = Paths.get(duplicados);

            // Crea el archivo duplicados.log si no existe
            if (!Files.exists(duplicatesFilePath)) {
                try {
                    Files.createDirectories(duplicatesFilePath.getParent());
                    Files.createFile(duplicatesFilePath);
                } catch (IOException e) {
                    System.err.println("Error al crear el archivo de duplicados: " + e.getMessage());
                    e.printStackTrace();
                    return;
                }
            }

            // Registra el registro duplicado en el archivo duplicados.log solo si no está ya presente
            List<String> existingLines = Files.readAllLines(duplicatesFilePath);
            String recordLine = Arrays.toString(rowData);
            boolean isDuplicate = existingLines.stream().anyMatch(line -> line.contains(recordLine));

            if (!isDuplicate) {
                try {
                    Files.write(duplicatesFilePath, (recordLine + System.lineSeparator()).getBytes(), StandardOpenOption.APPEND);
                    System.out.println("Registro duplicado registrado en duplicados.log");
                } catch (IOException e) {
                    System.err.println("Error al escribir en el archivo de duplicados: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            System.err.println("Error al cargar el archivo de propiedades: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    System.err.println("Error al cerrar el flujo de entrada: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }

    private static void handleSQLException(SQLException e) {
        logError(e);
    }

    private static void handleIOException(IOException e) {
        logError(e);
    }

    private static void logError(Exception e) {
        Properties prop = new Properties();
        InputStream input = null;

        try {
            input = new FileInputStream("src/main/resources/application.properties");
            prop.load(input);
            String errores = prop.getProperty("errors.file.path");
            Path errorsFilePath = Paths.get(errores);

            // Crea el archivo errores.log si no existe
            if (!Files.exists(errorsFilePath)) {
                try {
                    Files.createDirectories(errorsFilePath.getParent());
                    Files.createFile(errorsFilePath);
                } catch (IOException ex) {
                    System.err.println("Error al crear el archivo de errores: " + ex.getMessage());
                    ex.printStackTrace();
                    return;
                }
            }

            // Registra el error en el archivo errores.log
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            String errorStackTrace = sw.toString();

            try {
                Files.write(errorsFilePath, (errorStackTrace + System.lineSeparator()).getBytes(), StandardOpenOption.APPEND);
                System.out.println("Error registrado en errores.log");
            } catch (IOException ex) {
                System.err.println("Error al escribir en el archivo de errores: " + ex.getMessage());
                ex.printStackTrace();
            }
        } catch (IOException ex) {
            System.err.println("Error al cargar el archivo de propiedades: " + ex.getMessage());
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException ex) {
                    System.err.println("Error al cerrar el flujo de entrada: " + ex.getMessage());
                    ex.printStackTrace();
                }
            }
        }
    }
}
