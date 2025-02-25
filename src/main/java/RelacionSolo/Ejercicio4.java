package RelacionSolo;

import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import oracle.jdbc.OracleConnection;
import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;

public class Ejercicio4 {

    public static void main(String[] args) {
        try {
            String url = "jdbc:oracle:thin:@localhost:1521/XEPDB1";
            String user = "system";
            String password = "1234";
            Connection con = DriverManager.getConnection(url, user, password);

            //insertarInformatica(con);
            //insertarAdministracion(con);
            //insertarDireccion(con);
            //leerInformaticos(con);
            //leerTodo(con);
            //addDirectivo(con);
            //leerTodo(con);
            //borrarAdministrativo(con);
            //leerTodo(con);
            //borrarInformatica(con);
            //leerTodo(con);
        } catch (SQLException ex) {
            Logger.getLogger(Ejercicio4.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static void borrarInformatica(Connection con) throws SQLException{
        PreparedStatement del = con.prepareStatement("DELETE FROM GRUPOS G WHERE G.NOMBRE = 'INFORMATICA'");
        del.executeUpdate();
    }

    public static void borrarAdministrativo(Connection con) throws SQLException {
        OracleConnection oraCon = con.unwrap(OracleConnection.class);
        PreparedStatement consulta = con.prepareStatement("SELECT * FROM GRUPOS G WHERE G.NOMBRE = 'ADMINISTRACION'");
        List<Object> listaPersonas = new ArrayList<>();
        ResultSet rs = consulta.executeQuery();

        while (rs.next()) {
            String nombreGrupo = rs.getString(1);
            Array personas = rs.getArray(2);

            Object[] pArray = (Object[]) personas.getArray();

            for (Object obj : pArray) {
                Struct persona = (Struct) obj;
                Object[] pAtrib = persona.getAttributes();
                int codigo = ((Number) pAtrib[0]).intValue();
                
                if (codigo != 8) {
                    listaPersonas.add(persona);
                }
            }
        }

        Object[] nuevoArray = listaPersonas.toArray();
        ArrayDescriptor descriptor = ArrayDescriptor.createDescriptor("P_ARRAY", oraCon);
        ARRAY arrayFinal = new ARRAY(descriptor, oraCon, nuevoArray);

        PreparedStatement update = con.prepareStatement("UPDATE GRUPOS G SET G.PERSONAS = ? WHERE G.NOMBRE = 'ADMINISTRACION'");
        update.setArray(1, arrayFinal);

        update.executeUpdate();
    }

    public static void addDirectivo(Connection con) throws SQLException {
        OracleConnection oraCon = con.unwrap(OracleConnection.class);
        PreparedStatement consulta = con.prepareStatement("SELECT * FROM GRUPOS G WHERE G.NOMBRE = 'DIRECCION'");
        List<Object> listaPersonas = new ArrayList<>();
        ResultSet rs = consulta.executeQuery();

        while (rs.next()) {
            String nombreGrupo = rs.getString(1);
            Array personas = rs.getArray(2);

            Object[] pArray = (Object[]) personas.getArray();

            for (Object obj : pArray) {
                Struct persona = (Struct) obj;
                listaPersonas.add(persona);
            }
        }

        Struct d1 = con.createStruct("DIRECCION", new Object[]{"c/Paris", "Sevilla", 15001});
        Struct persona = con.createStruct("PERSONA", new Object[]{12, "Juana", d1, Date.valueOf("1976-02-18")});

        listaPersonas.add(persona);

        Object[] nuevoArray = listaPersonas.toArray();
        ArrayDescriptor descriptor = ArrayDescriptor.createDescriptor("P_ARRAY", oraCon);
        ARRAY arrayFinal = new ARRAY(descriptor, oraCon, nuevoArray);

        PreparedStatement update = con.prepareStatement("UPDATE GRUPOS G SET G.PERSONAS = ? WHERE G.NOMBRE = 'DIRECCION'");
        update.setArray(1, arrayFinal);

        update.executeUpdate();

    }

    public static void insertarDireccion(Connection con) throws SQLException {
        OracleConnection oraCon = con.unwrap(OracleConnection.class);

        Struct d1 = con.createStruct("DIRECCION", new Object[]{"c/Volga", "Sevilla", 15001});
        Struct d2 = con.createStruct("DIRECCION", new Object[]{"c/Tajo", "Malaga", 15001});

        Struct p1 = con.createStruct("PERSONA", new Object[]{10, "Jose Miguel", d1, Date.valueOf("1979-12-05")});
        Struct p2 = con.createStruct("PERSONA", new Object[]{11, "Antonia", d2, Date.valueOf("1980-05-17")});

        Object[] personas = {p1, p2};

        ArrayDescriptor descriptor = ArrayDescriptor.createDescriptor("P_ARRAY", oraCon);
        ARRAY personasArray = new ARRAY(descriptor, oraCon, personas);

        PreparedStatement insertar = con.prepareStatement("INSERT INTO GRUPOS VALUES(?, ?)");

        insertar.setString(1, "DIRECCION");
        insertar.setArray(2, personasArray);

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

        ArrayDescriptor descriptor = ArrayDescriptor.createDescriptor("P_ARRAY", oraCon);
        ARRAY personasArray = new ARRAY(descriptor, oraCon, personas);

        PreparedStatement insertar = con.prepareStatement("INSERT INTO GRUPOS VALUES(?, ?)");

        insertar.setString(1, "ADMINISTRACION");
        insertar.setArray(2, personasArray);

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

        ArrayDescriptor descriptor = ArrayDescriptor.createDescriptor("P_ARRAY", oraCon);
        ARRAY personasArray = new ARRAY(descriptor, oraCon, personas);

        PreparedStatement insertar = con.prepareStatement("INSERT INTO GRUPOS VALUES(?, ?)");

        insertar.setString(1, "INFORMATICA");
        insertar.setArray(2, personasArray);

        int i = insertar.executeUpdate();

        System.out.println("Filas insertadas: " + i);

    }

    public static void leerInformaticos(Connection con) throws SQLException {
        PreparedStatement consulta = con.prepareStatement("SELECT * FROM GRUPOS G where G.NOMBRE = 'INFORMATICA'");
        ResultSet rs = consulta.executeQuery();

        while (rs.next()) {
            String nombreGrupo = rs.getString(1);
            Array personas = (Array) rs.getObject(2);

            Object[] aPersonas = (Object[]) personas.getArray();

            for (Object obj : aPersonas) {
                Struct pers = (Struct) obj;
                Object[] atribPers = pers.getAttributes();

                int codigo = ((Number) atribPers[0]).intValue();
                String nombre = (String) atribPers[1];

                Struct direccion = (Struct) atribPers[2];
                Object[] dAtrib = direccion.getAttributes();

                String calle = (String) dAtrib[0];
                String ciudad = (String) dAtrib[1];
                int codPostal = ((Number) dAtrib[2]).intValue();

                Timestamp fNac = (Timestamp) atribPers[3];

                System.out.println("Grupo: " + nombreGrupo + " | Codigo: " + codigo + " | Nombre: " + nombre + " | F.Nac: " + fNac
                        + " | Calle: " + calle + " | Ciudad" + ciudad + " | Cod.Postal: " + codPostal);

            }

        }
    }

    public static void leerTodo(Connection con) throws SQLException {
        PreparedStatement consulta = con.prepareStatement("SELECT * FROM GRUPOS G");
        ResultSet rs = consulta.executeQuery();

        while (rs.next()) {
            String nombreGrupo = rs.getString(1);
            Array personas = (Array) rs.getObject(2);

            Object[] aPersonas = (Object[]) personas.getArray();

            for (Object obj : aPersonas) {
                Struct pers = (Struct) obj;
                Object[] atribPers = pers.getAttributes();

                int codigo = ((Number) atribPers[0]).intValue();
                String nombre = (String) atribPers[1];

                Struct direccion = (Struct) atribPers[2];
                Object[] dAtrib = direccion.getAttributes();

                String calle = (String) dAtrib[0];
                String ciudad = (String) dAtrib[1];
                int codPostal = ((Number) dAtrib[2]).intValue();

                Timestamp fNac = (Timestamp) atribPers[3];

                System.out.println("Grupo: " + nombreGrupo + " | Codigo: " + codigo + " | Nombre: " + nombre + " | F.Nac: " + fNac
                        + " | Calle: " + calle + " | Ciudad" + ciudad + " | Cod.Postal: " + codPostal);

            }

        }
    }
}
