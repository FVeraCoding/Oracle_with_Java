package RelacionEjercicios;

import java.sql.*;

public class Ejercicio1 {

    public static void main(String[] args) throws SQLException {

        // 🔹 Paso 1: Establecer conexión
        String url = "jdbc:oracle:thin:@localhost:1521/XEPDB1";
        String user = "system";
        String password = "1234";
        Connection con = DriverManager.getConnection(url, user, password);
        con.setAutoCommit(false); // Desactivar auto-commit para transacción

      /*  // 🔹 Paso 2: Preparar la sentencia de inserción
        String sql = "INSERT INTO ALUMNOS2 VALUES(T_ALUMNO(?, ?, ?, ?))";
        PreparedStatement sentencia = con.prepareStatement(sql);

        // 🔹 Insertar varios alumnos
        insertarAlumno(con, sentencia, 1, "Fernando", "Calle Alta", "Sevilla", 41005, "1999-11-02", 9, 8, 10);
        insertarAlumno(con, sentencia, 2, "Ana", "Avenida del Sol", "Guadalajara", 19001, "2000-05-14", 6, 7, 9);
        insertarAlumno(con, sentencia, 3, "Luis", "Calle Mayor", "Madrid", 28001, "1998-07-22", 5, 6, 7);
        insertarAlumno(con, sentencia, 4, "Marta", "Plaza Central", "Guadalajara", 19002, "2001-02-11", 7, 8, 9);
        insertarAlumno(con, sentencia, 5, "Carlos", "Calle Luna", "Barcelona", 21092, "1997-10-30", 3, 4, 5);
        insertarAlumno(con, sentencia, 6, "Beatriz", "Calle Verde", "Sevilla", 41006, "1999-06-17", 10, 9, 9);
        insertarAlumno(con, sentencia, 7, "Javier", "Avenida del Mar", "Guadalajara", 19003, "1996-12-25", 8, 8, 7);
        insertarAlumno(con, sentencia, 8, "Elena", "Calle Roble", "Madrid", 28002, "1995-09-05", 5, 5, 5);
        insertarAlumno(con, sentencia, 9, "Pablo", "Calle del Río", "Valencia", 46001, "2002-01-15", 9, 9, 10);

        // 🔹 Confirmar inserción
        con.commit();
        System.out.println("✅ Inserciones completadas correctamente.");

        // 🔹 Cerrar recursos
        sentencia.close();
        con.close();
    }

    // 🔹 Método para insertar alumnos
    private static void insertarAlumno(Connection con, PreparedStatement sentencia, int codigo, String nombre,
                                       String calle, String ciudad, int codigoPostal, String fechaNacimiento,
                                       int n1, int n2, int n3) throws SQLException {
        Struct direccion = con.createStruct("DIRECCION", new Object[]{calle, ciudad, codigoPostal});
        Struct persona = con.createStruct("PERSONA", new Object[]{codigo, nombre, direccion, Date.valueOf(fechaNacimiento)});

        sentencia.setObject(1, persona);
        sentencia.setInt(2, n1);
        sentencia.setInt(3, n2);
        sentencia.setInt(4, n3);

        sentencia.executeUpdate();
        System.out.println("🔹 Insertado: " + nombre + " (" + ciudad + ") - Notas: " + n1 + ", " + n2 + ", " + n3);*/
        
PreparedStatement leerTodo = con.prepareStatement("SELECT * FROM GRUPOS");
            ResultSet rs2 = leerTodo.executeQuery();

            while (rs2.next()) {
                String nombre = rs2.getString(1);
                Array personas = rs2.getArray(2);
                Object[] arrayPersonas2 = (Object[]) personas.getArray();

                System.out.println(nombre);
                for (Object pers : arrayPersonas2) {
                    Struct personaStruct = (Struct) pers;
                    Object[] atributos = personaStruct.getAttributes();

                    int codigo = ((Number) atributos[0]).intValue();
                    String nombrePers = (String) atributos[1];

                    Struct direcc = (Struct) atributos[2];
                    Object[] atributosDirecc = direcc.getAttributes();

                    String calle = (String) atributosDirecc[0];
                    String ciudad = (String) atributosDirecc[1];
                    int codPostal = ((Number) atributosDirecc[2]).intValue();
                    Timestamp fNac = (Timestamp) atributos[3];

                    System.out.println("Codigo: " + codigo + " | Nombre: " + nombrePers + " | Calle: " + calle + " | Ciudad: " + ciudad + " Cod.Postal: " + codPostal + " | F. Nac: " + fNac);
                }
            }
        
    }
}
