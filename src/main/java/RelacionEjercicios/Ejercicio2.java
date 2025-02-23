package RelacionEjercicios;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Timestamp;

public class Ejercicio2 {

    public static void main(String[] args) throws SQLException {
        //Crear conexión con BBDD.
        String url = "jdbc:oracle:thin:@localhost:1521/XEPDB1";
        String user = "system";
        String password = "1234";
        Connection con = DriverManager.getConnection(url, user, password);
        
        //Preparar la consulta
        
        //1. Nombre del Alumno y nota media.
        PreparedStatement consulta = con.prepareStatement("SELECT A.PERS.NOMBRE, A.PERS.DIREC.CIUDAD, A.NOTA_MEDIA() FROM ALUMNOS2 A");
        
        //Ejecutar la consulta
        ResultSet rs = consulta.executeQuery();
        
        while(rs.next()){
            String nombre = rs.getString(1);
            String ciudad = rs.getString(2);
            double notaMedia = rs.getDouble(3);
            
            
            System.out.println("Alumno: "+nombre+ " - Ciudad: "+ ciudad +  " - Nota media: "+notaMedia);
        }
        System.out.println("-----------------------------------------------------------------------------------");
        
        //2. Alumnos de Guadalajara con nota media mayor de 6.
        PreparedStatement consulta2 = con.prepareStatement("Select A.PERS, A.NOTA_MEDIA() from ALUMNOS2 A where A.PERS.DIREC.CIUDAD = 'Guadalajara' AND A.NOTA_MEDIA() > 6");
        ResultSet rs2 = consulta2.executeQuery();
        
        while(rs2.next()){
            Struct persona = (Struct) rs2.getObject(1);
            Object[] atributosPersona = persona.getAttributes();
            
            int codigo = ( (Number)atributosPersona[0]).intValue();
            String nombre = (String) atributosPersona[1];
            
            Struct direccion = (Struct) atributosPersona[2];
            Object[] atributosDireccion = direccion.getAttributes();
            String calle = (String) atributosDireccion[0];
            String ciudad = (String) atributosDireccion[1];
            int codigoPostal = ( (Number) atributosDireccion[2]).intValue();
            
            Timestamp fechaNac = (Timestamp) atributosPersona[3];
            
            double notaMedia = rs2.getDouble(2);
            
            
            System.out.println("Persona: Codigo: "+codigo + " Nombre: "+nombre+ " - F. Nac: "+fechaNac + " - Nota media: "+ notaMedia);
            System.out.println("Direccion: "+calle+ " - "+ciudad+" - "+codigoPostal);
            System.out.println("---------------------------------------------------");
        }
        
        
        //3. Nombre Alumno con más nota media.
        PreparedStatement consulta3 = con.prepareStatement("SELECT A.PERS.NOMBRE, A.NOTA_MEDIA() FROM ALUMNOS2 A WHERE A.NOTA_MEDIA() = (SELECT MAX(A2.NOTA_MEDIA()) FROM ALUMNOS2 A2)");
        ResultSet rs3 = consulta3.executeQuery();
        while (rs3.next()){
            String nombre = rs3.getString(1);
            double notaMedia = rs3.getDouble(2);
            System.out.println("Alumno con mayor nota media: "+nombre + " - Nota media: "+notaMedia);
        }
        
        //4. Nombre Alumno con la nota más alta.
        PreparedStatement consulta4 = con.prepareStatement("SELECT A.PERS.NOMBRE, A.NOTA_MAS_ALTA() FROM ALUMNOS2 A WHERE A.NOTA_MAS_ALTA() = (SELECT MAX(A2.NOTA_MAS_ALTA()) FROM ALUMNOS2 A2)");
        ResultSet rs4 = consulta4.executeQuery();
        
        while(rs4.next()){
            String nombre = rs4.getString(1);
            double notaMasAlta = rs4.getDouble(2);
            System.out.println("Alumno con la nota mas alta: "+nombre + " - Nota mas alta: "+notaMasAlta);
        }
        
        
        
    }
}
