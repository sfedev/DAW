package des.pruebajdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Db_EjercicioGranjero1 {

	static final String URL = "jdbc:mysql://localhost:3306/Farmville";
    static final String USER = "root";
    static final String PASSWORD = "Limon@123";

    public static void main(String[] args) {
        try {
            // Crear un registro de granjero
            int granjeroId = crearGranjero("Sergio", 50, 50);
            System.out.println("Se ha creado un granjero con ID: " + granjeroId);

            // Recuperar el ID del granjero creado
            int idRecuperado = obtenerIdPorNombre("Juan");
            System.out.println("El ID del granjero con nombre 'Juan' es: " + idRecuperado);

            // Actualizar puntos y dinero del granjero por ID
            actualizarPuntosDineroPorId(granjeroId, 10, 10);
            System.out.println("Se han actualizado puntos y dinero del granjero con ID " + granjeroId);

            // Actualizar puntos y dinero de otros granjeros con el mismo nombre
            actualizarPuntosDineroPorNombre("Juan", 100, 100);
            System.out.println("Se han actualizado puntos y dinero de otros granjeros con el nombre 'Juan'");

            // Insertar un registro en la tabla Plantaciones
            insertarPlantacion(idRecuperado, 20.0); // 20.0 es el precio de compra inicial

            // Actualizar precios de compra de plantaciones para granjeros con el mismo nombre
            actualizarPreciosCompraPorNombre("Juan", 10.0); // 10.0 es el incremento en el precio de compra

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static int crearGranjero(String nombre, int puntos, int dinero) throws SQLException {
        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement statement = connection.prepareStatement("INSERT INTO granjeros (nombre, puntos, dinero) VALUES (?, ?, ?)", Statement.RETURN_GENERATED_KEYS)) {
            statement.setString(1, nombre);
            statement.setInt(2, puntos);
            statement.setInt(3, dinero);

            int affectedRows = statement.executeUpdate();
            if (affectedRows == 0) {
                throw new SQLException("No se ha podido crear el granjero.");
            }

            try (ResultSet generatedKeys = statement.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    return generatedKeys.getInt(1);
                } else {
                    throw new SQLException("No se ha podido obtener el ID del granjero.");
                }
            }
        }
    }

    private static int obtenerIdPorNombre(String nombre) throws SQLException {
        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement statement = connection.prepareStatement("SELECT id FROM granjeros WHERE nombre = ?")) {
            statement.setString(1, nombre);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getInt("id");
                } else {
                    throw new SQLException("No se ha encontrado ningún granjero con el nombre '" + nombre + "'.");
                }
            }
        }
    }

    private static void actualizarPuntosDineroPorId(int id, int puntos, int dinero) throws SQLException {
        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement statement = connection.prepareStatement("UPDATE granjeros SET puntos = puntos + ?, dinero = dinero + ? WHERE id = ?")) {
            statement.setInt(1, puntos);
            statement.setInt(2, dinero);
            statement.setInt(3, id);

            int affectedRows = statement.executeUpdate();
            if (affectedRows == 0) {
                throw new SQLException("No se ha actualizado ningún granjero con el ID " + id);
            }
        }
    }

    private static void actualizarPuntosDineroPorNombre(String nombre, int puntos, int dinero) throws SQLException {
        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement statement = connection.prepareStatement("UPDATE granjeros SET puntos = puntos + ?, dinero = dinero + ? WHERE nombre = ?")) {
            statement.setInt(1, puntos);
            statement.setInt(2, dinero);
            statement.setString(3, nombre);

            statement.executeUpdate();
        }
    }
    
    private static void insertarPlantacion(int granjeroId, double precioCompra) throws SQLException {
        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement statement = connection.prepareStatement("INSERT INTO plantaciones (granjero_id, precio_compra) VALUES (?, ?)")) {
            statement.setInt(1, granjeroId);
            statement.setDouble(2, precioCompra);

            statement.executeUpdate();
        }
    }

    private static void actualizarPreciosCompraPorNombre(String nombre, double incremento) throws SQLException {
        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement statement = connection.prepareStatement(
                     "UPDATE plantaciones " +
                             "SET precio_compra = precio_compra + ? " +
                             "WHERE granjero_id IN (SELECT id FROM granjeros WHERE nombre = ?)")) {
            statement.setDouble(1, incremento);
            statement.setString(2, nombre);

            statement.executeUpdate();
        }
    }
}