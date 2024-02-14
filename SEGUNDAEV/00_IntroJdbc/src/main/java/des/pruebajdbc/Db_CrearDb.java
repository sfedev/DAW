package des.pruebajdbc;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.DriverManager;
import java.util.Scanner;

public class Db_CrearDb {

	public static void main(String[] args) {
        Connection conexion = null;
        Statement statement = null;
        Scanner kb = new Scanner(System.in);

        try {
            // Cargar el controlador JDBC
            Class.forName("com.mysql.cj.jdbc.Driver");

            // Establecer la conexión con la base de datos
            String url = "jdbc:mysql://localhost:3306/";
            String usuario = "root";
            String contraseña = "Limon@123";
            conexion = DriverManager.getConnection(url, usuario, contraseña);

            // Realizar operaciones con la base de datos aquí...
            System.out.println("Conexión exitosa.");
            
            // Preparamos la declaración
            statement = conexion.createStatement();
            System.out.print("Introduce el nombre de la base de datos -> ");
            
            String dbName = kb.next();
            String createDatabase =  "CREATE DATABASE " + dbName;
            statement.executeUpdate(createDatabase);
            System.out.println("Se ha creado la base de datos " + dbName);
            

        } catch (ClassNotFoundException e) {
            System.err.println("Error al cargar el controlador JDBC: " + e.getMessage());
        } catch (SQLException e) {
            System.err.println("Error al establecer la conexión: " + e.getMessage());
        } finally {
            // Cerrar la conexión en el bloque finally para asegurarse de liberar recursos
            try {
            	if (statement != null) {
                     statement.close();
                 }
                if (conexion != null && !conexion.isClosed()) {
                    conexion.close();
                }
            } catch (SQLException e) {
                System.err.println("Error al cerrar la conexión: " + e.getMessage());
            }
        }
    }

}
