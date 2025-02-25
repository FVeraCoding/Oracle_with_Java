package RelacionSolo;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Timestamp;
import java.util.logging.Level;
import java.util.logging.Logger;
import oracle.jdbc.OracleConnection;
import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;

public class Ejercicio5 {

    public static void main(String[] args) {
        try {
            String url = "jdbc:oracle:thin:@localhost:1521/XEPDB1";
            String user = "system";
            String password = "1234";
            Connection con = DriverManager.getConnection(url, user, password);

            //insertarDireccion(con);
            //insertarInformatica(con);
            //insertarAdministracion(con);
            gruposMalaga(con);

        } catch (SQLException ex) {
            Logger.getLogger(Ejercicio5.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public static void gruposMalaga(Connection con) throws SQLException {
        PreparedStatement consulta = con.prepareStatement("SELECT G.ID, G.NOMBRE, G.FECHA_CREACION FROM GRUPOS2 G, TABLE(G.PERSONAS) P WHERE P.DIREC.CIUDAD = 'Malaga'");
        ResultSet rs = consulta.executeQuery();

        while (rs.next()) {
            int id = rs.getInt(1);
            String nombre = rs.getString(2);
            Timestamp fCreac = rs.getTimestamp(3);

            System.out.println("Codigo: " + id + " | Nombre: " + nombre + " | F.Creacion: " + fCreac);
        }
    }

    public static void insertarDireccion(Connection con) throws SQLException {
        OracleConnection oraCon = con.unwrap(OracleConnection.class);

        Struct d1 = con.createStruct("DIRECCION", new Object[]{"c/Volga", "Sevilla", 15001});
        Struct d2 = con.createStruct("DIRECCION", new Object[]{"c/Tajo", "Malaga", 15001});

        Struct p1 = con.createStruct("PERSONA", new Object[]{10, "Jose Miguel", d1, Date.valueOf("1979-12-05")});
        Struct p2 = con.createStruct("PERSONA", new Object[]{11, "Antonia", d2, Date.valueOf("1980-05-17")});

        Object[] personas = {p1, p2};

        ArrayDescriptor descriptor = ArrayDescriptor.createDescriptor("TA_PERSONA", oraCon);
        ARRAY personasArray = new ARRAY(descriptor, oraCon, personas);

        PreparedStatement insertar = con.prepareStatement("INSERT INTO GRUPOS2 VALUES(?, ?, ?, ?)");

        insertar.setInt(1, 3);
        insertar.setString(2, "DIRECCION");
        insertar.setArray(3, personasArray);
        insertar.setDate(4, Date.valueOf("1996-02-18"));
        int i = insertar.executeUpdate();

        System.out.println("Filas insertadas: " + i);
    }

    public static void insertarAdministracion(Connection con) throws SQLException {
        OracleConnection oraCon = con.unwrap(OracleConnection.class);

        Struct d1 = con.createStruct("DIRECCION", new Object[]{"c/España", "Granada", 11001});
        Struct d2 = con.createStruct("DIRECCION", new Object[]{"c/Conde", "Malaga", 12001});
        Struct d3 = con.createStruct("DIRECCION", new Object[]{"c/Mimbre", "Granada", 11001});
        Struct d4 = con.createStruct("DIRECCION", new Object[]{"c/Segura", "Sevilla", 16001});

        Struct p1 = con.createStruct("PERSONA", new Object[]{6, "Lucas", d1, Date.valueOf("1988-01-29")});
        Struct p2 = con.createStruct("PERSONA", new Object[]{7, "Marta", d2, Date.valueOf("1986-07-30")});
        Struct p3 = con.createStruct("PERSONA", new Object[]{8, "Carmen", d3, Date.valueOf("1990-04-01")});
        Struct p4 = con.createStruct("PERSONA", new Object[]{9, "Milagros", d4, Date.valueOf("1994-01-07")});

        Object[] personas = {p1, p2, p3, p4};

        ArrayDescriptor descriptor = ArrayDescriptor.createDescriptor("TA_PERSONA", oraCon);
        ARRAY personasArray = new ARRAY(descriptor, oraCon, personas);

        PreparedStatement insertar = con.prepareStatement("INSERT INTO GRUPOS2 VALUES(?, ?, ?, ?)");

        insertar.setInt(1, 2);
        insertar.setString(2, "ADMINISTRACION");
        insertar.setArray(3, personasArray);
        insertar.setDate(4, Date.valueOf("2010-02-18"));
        int i = insertar.executeUpdate();

        System.out.println("Filas insertadas: " + i);

    }

    public static void insertarInformatica(Connection con) throws SQLException {
        OracleConnection oraCon = con.unwrap(OracleConnection.class);

        Struct d1 = con.createStruct("DIRECCION", new Object[]{"c/Conde", "Malaga", 12001});
        Struct d2 = con.createStruct("DIRECCION", new Object[]{"c/Amiel", "Cordoba", 16001});
        Struct d3 = con.createStruct("DIRECCION", new Object[]{"c/Castillo", "Cadiz", 15001});
        Struct d4 = con.createStruct("DIRECCION", new Object[]{"c/Sin nombre", "Cadiz", 11001});
        Struct d5 = con.createStruct("DIRECCION", new Object[]{"c/Cuba", "Malaga", 10001});

        Struct p1 = con.createStruct("PERSONA", new Object[]{1, "Juan", d1, Date.valueOf("1990-02-17")});
        Struct p2 = con.createStruct("PERSONA", new Object[]{2, "Maria", d2, Date.valueOf("1991-10-21")});
        Struct p3 = con.createStruct("PERSONA", new Object[]{3, "Sofia", d3, Date.valueOf("1992-09-01")});
        Struct p4 = con.createStruct("PERSONA", new Object[]{4, "Carla", d4, Date.valueOf("1990-08-14")});
        Struct p5 = con.createStruct("PERSONA", new Object[]{5, "Manuel", d5, Date.valueOf("1989-02-27")});

        Object[] personas = {p1, p2, p3, p4, p5};

        ArrayDescriptor descriptor = ArrayDescriptor.createDescriptor("TA_PERSONA", oraCon);
        ARRAY personasArray = new ARRAY(descriptor, oraCon, personas);

        PreparedStatement insertar = con.prepareStatement("INSERT INTO GRUPOS2 VALUES(?, ?, ?, ?)");

        insertar.setInt(1, 1);
        insertar.setString(2, "INFORMATICA");
        insertar.setArray(3, personasArray);
        insertar.setDate(4, Date.valueOf("2023-02-18"));
        int i = insertar.executeUpdate();

        System.out.println("Filas insertadas: " + i);

    }
}
