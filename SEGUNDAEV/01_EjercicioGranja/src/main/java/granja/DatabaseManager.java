package granja;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.io.InputStream;

public class DatabaseManager {

    public static Connection obtenerConexion() throws SQLException {
        Properties prop = new Properties();
        try (InputStream input = new FileInputStream("src/main/resources/application.properties")) {
            prop.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }

        String dbUrl = prop.getProperty("db.url");
        String dbUser = prop.getProperty("db.user");
        String dbPassword = prop.getProperty("db.password");

        return DriverManager.getConnection(dbUrl, dbUser, dbPassword);
    }
}
