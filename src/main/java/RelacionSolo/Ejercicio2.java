package RelacionSolo;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Timestamp;

public class Ejercicio2 {

    public static void main(String[] args) throws SQLException {
        String url = "jdbc:oracle:thin:@localhost:1521/XEPDB1";
        String user = "system";
        String password = "1234";
        Connection con = DriverManager.getConnection(url, user, password);

        /*insertarAlumno(con, "C/Mayor", "GUADALAJARA", 19001, 4, "Pedro López", "2000-05-14", 7, 8, 6);
        insertarAlumno(con, "Av. Castilla", "GUADALAJARA", 19002, 5, "Lucía Fernández", "2001-08-22", 9, 7, 8);
        insertarAlumno(con, "C/Real", "Madrid", 28001, 6, "Carlos Pérez", "1999-12-10", 5, 6, 7);
        insertarAlumno(con, "Paseo del Prado", "Sevilla", 41003, 7, "Ana Torres", "2002-03-05", 10, 9, 8);*/
        
        //nombreNotaMedia(con);
        //verGuadalajara(con);
        //mayorNotaMedia(con);
        notaMasAlta(con);
    }

    public static void insertarAlumno(Connection con, String nombreCalle, String ciudad, int codPostal, int codigoPers,
            String nombrePers, String fechaNac, int n1, int n2, int n3) throws SQLException {
        PreparedStatement consulta1 = con.prepareStatement("INSERT INTO ALUMNOS2 VALUES(?, ?, ?, ?)");

        Struct direccion1 = con.createStruct("DIRECCION", new Object[]{nombreCalle, ciudad, codPostal});
        Struct persona1 = con.createStruct("PERSONA", new Object[]{codigoPers, nombrePers, direccion1, Date.valueOf(fechaNac)});

        consulta1.setObject(1, persona1);
        consulta1.setInt(2, n1);
        consulta1.setInt(3, n2);
        consulta1.setInt(4, n3);

        consulta1.executeUpdate();

        consulta1.close();
    }

    public static void nombreNotaMedia(Connection con) throws SQLException {
        PreparedStatement consulta = con.prepareStatement("SELECT A.PERS.NOMBRE, A.NOTA_MEDIA() FROM ALUMNOS2 A");
        ResultSet rs = consulta.executeQuery();
        while (rs.next()) {
            String nombre = rs.getString(1);
            double notaMedia = rs.getDouble(2);

            System.out.println("Nombre: " + nombre + " | Nota media: " + notaMedia);
        }
    }

    public static void verGuadalajara(Connection con) throws SQLException {
        PreparedStatement consulta = con.prepareStatement("SELECT A.PERS, A.NOTA_MEDIA() FROM ALUMNOS2 A WHERE A.PERS.DIREC.CIUDAD = 'GUADALAJARA' AND A.NOTA_MEDIA() > 6");
        ResultSet rs = consulta.executeQuery();

        while (rs.next()) {
            Struct persona = (Struct) rs.getObject(1);
            Object[] atributosPers = persona.getAttributes();

            int codigo = ((Number) atributosPers[0]).intValue();
            String nombre = (String) atributosPers[1];

            Struct direccion = (Struct) atributosPers[2];
            Object[] atributosDir = direccion.getAttributes();

            String calle = (String) atributosDir[0];
            String ciudad = (String) atributosDir[1];
            int codPostal = ((Number) atributosDir[2]).intValue();

            Timestamp fNac = (Timestamp) atributosPers[3];

            System.out.println("Persona: Codigo: " + codigo + " | Nombre: " + nombre + " | F.Nac: " + fNac + " | Calle: " + calle + " | Ciudad: " + ciudad + " | Cod Postal: " + codPostal);
        }

    }

    public static void mayorNotaMedia(Connection con) throws SQLException {
        PreparedStatement consulta = con.prepareStatement("SELECT A.PERS, A.NOTA_MEDIA() FROM ALUMNOS2 A WHERE A.NOTA_MEDIA() = (SELECT MAX(A2.NOTA_MEDIA()) FROM ALUMNOS2 A2)");
        ResultSet rs = consulta.executeQuery();

        while (rs.next()) {
            Struct persona = (Struct) rs.getObject(1);
            Object[] atributosPers = persona.getAttributes();

            int codigo = ((Number) atributosPers[0]).intValue();
            String nombre = (String) atributosPers[1];

            Struct direccion = (Struct) atributosPers[2];
            Object[] atributosDir = direccion.getAttributes();

            String calle = (String) atributosDir[0];
            String ciudad = (String) atributosDir[1];
            int codPostal = ((Number) atributosDir[2]).intValue();

            Timestamp fNac = (Timestamp) atributosPers[3];
            
            double notaMedia = rs.getDouble(2);

            System.out.println("Persona: Codigo: " + codigo + " | Nombre: " + nombre + " | F.Nac: " + fNac + " | Calle: " + calle + " | Ciudad: " + ciudad + " | Cod Postal: " + codPostal + " | Nota media: "+notaMedia);
        }
    }
    
    public static void notaMasAlta(Connection con) throws SQLException{
        PreparedStatement consulta = con.prepareStatement("SELECT A.PERS, A.NOTA_MAS_ALTA() FROM ALUMNOS2 A WHERE A.NOTA_MAS_ALTA() = (SELECT MAX(A2.NOTA_MAS_ALTA())FROM ALUMNOS2 A2)");
        ResultSet rs = consulta.executeQuery();

        while (rs.next()) {
            Struct persona = (Struct) rs.getObject(1);
            Object[] atributosPers = persona.getAttributes();

            int codigo = ((Number) atributosPers[0]).intValue();
            String nombre = (String) atributosPers[1];

            Struct direccion = (Struct) atributosPers[2];
            Object[] atributosDir = direccion.getAttributes();

            String calle = (String) atributosDir[0];
            String ciudad = (String) atributosDir[1];
            int codPostal = ((Number) atributosDir[2]).intValue();

            Timestamp fNac = (Timestamp) atributosPers[3];
            
            double notaMedia = rs.getDouble(2);

            System.out.println("Persona: Codigo: " + codigo + " | Nombre: " + nombre + " | F.Nac: " + fNac + " | Calle: " + calle + " | Ciudad: " + ciudad + " | Cod Postal: " + codPostal + " | Nota mas alta: "+notaMedia);
        }
    }
}
