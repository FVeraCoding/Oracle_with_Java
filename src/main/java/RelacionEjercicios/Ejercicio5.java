package RelacionEjercicios;

import java.sql.*;
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

            //insertarInformatica(con);
            //insertarAdministracion(con);
            //insertarDireccion(con);
            //leerInformacion(con);
            //infoMalaga(con);
            ochenteros(con);
        } catch (SQLException ex) {
            Logger.getLogger(Ejercicio5.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void insertarInformatica(Connection con) throws SQLException {
        OracleConnection oraCon = con.unwrap(OracleConnection.class);

        PreparedStatement insert = con.prepareStatement("INSERT INTO GRUPOS2 G VALUES(?, ?, ?, ?)");

        //INFORMATICA
        Struct d1 = con.createStruct("DIRECCION", new Object[]{"C/Conde", "Malaga", 12001});
        Struct d2 = con.createStruct("DIRECCION", new Object[]{"C/Amiel", "Cordoba", 16001});
        Struct d3 = con.createStruct("DIRECCION", new Object[]{"C/Castillo", "Cadiz", 15001});
        Struct d4 = con.createStruct("DIRECCION", new Object[]{"C/Sin nombre", "Cadiz", 12001});
        Struct d5 = con.createStruct("DIRECCION", new Object[]{"C/Cuba", "Malaga", 10001});

        Struct pi1 = con.createStruct("PERSONA", new Object[]{1, "Juan", d1, Date.valueOf("1990-02-17")});
        Struct pi2 = con.createStruct("PERSONA", new Object[]{2, "Maria", d2, Date.valueOf("1991-10-21")});
        Struct pi3 = con.createStruct("PERSONA", new Object[]{3, "Sofia", d3, Date.valueOf("1992-09-01")});
        Struct pi4 = con.createStruct("PERSONA", new Object[]{4, "Carla", d4, Date.valueOf("1990-08-14")});
        Struct pi5 = con.createStruct("PERSONA", new Object[]{5, "Manuel", d5, Date.valueOf("1989-02-27")});

        Object[] personasArray = {pi1, pi2, pi3, pi4, pi5};
        ArrayDescriptor descriptor = ArrayDescriptor.createDescriptor("T_PERSONAS", oraCon);

        ARRAY arrayPersonas2 = new ARRAY(descriptor, oraCon, personasArray);

        insert.setInt(1, 1);
        insert.setString(2, "INFORMATICA");
        insert.setObject(3, arrayPersonas2);
        insert.setDate(4, Date.valueOf("2024-10-13"));

        insert.executeUpdate();
    }

    public static void insertarAdministracion(Connection con) throws SQLException {
        OracleConnection oraCon = con.unwrap(OracleConnection.class);

        PreparedStatement insert = con.prepareStatement("INSERT INTO GRUPOS2 G VALUES(?, ?, ?, ?)");

        //ADMINISTRACION
        Struct d6 = con.createStruct("DIRECCION", new Object[]{"c/Espania", "Granada", 11001});
        Struct d7 = con.createStruct("DIRECCION", new Object[]{"c/Conde", "Malaga", 12001});
        Struct d8 = con.createStruct("DIRECCION", new Object[]{"c/Mimbre", "Granada", 11001});
        Struct d9 = con.createStruct("DIRECCION", new Object[]{"c/Segura", "Sevilla", 16001});

        Struct pa1 = con.createStruct("PERSONA", new Object[]{6, "Lucas", d6, Date.valueOf("1988-01-29")});
        Struct pa2 = con.createStruct("PERSONA", new Object[]{7, "Marta", d7, Date.valueOf("1986-07-30")});
        Struct pa3 = con.createStruct("PERSONA", new Object[]{8, "Carmen", d8, Date.valueOf("1990-04-01")});
        Struct pa4 = con.createStruct("PERSONA", new Object[]{9, "Milagros", d9, Date.valueOf("1994-05-17")});

        Object[] personasAdministracion = {pa1, pa2, pa3, pa4};

        ArrayDescriptor descriptor = ArrayDescriptor.createDescriptor("T_PERSONAS", oraCon);

        ARRAY arrayPersonas2 = new ARRAY(descriptor, oraCon, personasAdministracion);

        insert.setInt(1, 2);
        insert.setString(2, "ADMINISTRACION");
        insert.setObject(3, arrayPersonas2);
        insert.setDate(4, Date.valueOf("2024-10-13"));

        insert.executeUpdate();
    }

    public static void insertarDireccion(Connection con) throws SQLException {
        OracleConnection oraCon = con.unwrap(OracleConnection.class);

        PreparedStatement insert = con.prepareStatement("INSERT INTO GRUPOS2 G VALUES(?, ?, ?, ?)");

        //DIRECCION
        Struct d10 = con.createStruct("DIRECCION", new Object[]{"c/Volga", "Sevilla", 15001});
        Struct d11 = con.createStruct("DIRECCION", new Object[]{"c/Tajo", "Malaga", 15001});

        Struct pd1 = con.createStruct("PERSONA", new Object[]{10, "Jose Miguel", d10, Date.valueOf("1979-12-05")});
        Struct pd2 = con.createStruct("PERSONA", new Object[]{11, "Antonia", d11, Date.valueOf("1980-05-17")});

        Object[] personasDireccion = {pd1, pd2};
        ArrayDescriptor descriptorDir = ArrayDescriptor.createDescriptor("T_PERSONAS", oraCon);
        ARRAY arrayPersonasDir = new ARRAY(descriptorDir, oraCon, personasDireccion);

        insert.setInt(1, 3);
        insert.setString(2, "DIRECCION");
        insert.setObject(3, arrayPersonasDir);
        insert.setDate(4, Date.valueOf("2024-10-13"));

        insert.executeUpdate();
    }

    public static void leerInformacion(Connection con) throws SQLException {
        PreparedStatement consulta = con.prepareStatement("SELECT * FROM GRUPOS2");
        ResultSet rs = consulta.executeQuery();

        while (rs.next()) {
            int codGrupo = rs.getInt(1);
            String nombreGrupos = rs.getString(2);

            Array personasArray = (Array) rs.getObject(3);
            Object[] personas = (Object[]) personasArray.getArray();

            for (Object p : personas) {
                Struct persona = (Struct) p;

                Object[] atributos = persona.getAttributes();

                int codigo = ((Number) atributos[0]).intValue();
                String nombre = (String) atributos[1];

                Struct direccion = (Struct) atributos[2];
                Object[] atributosDir = direccion.getAttributes();

                String ciudad = (String) atributosDir[1];

                System.out.println("Codigo: " + codigo + " | Nombre: " + nombre + " Ciudad: " + ciudad);
            }
        }
    }

    /*public static void consultaTodoMalaga() {
        try {
            // Oracle
            Connection conexion = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521/XEPDB1", "system", "1234");
            // Preparamos la consulta
            String sql = "SELECT * FROM GRUPOS2 GR, TABLE(PERSONAS) P WHERE P.DIREC.CIUDAD='Málaga'";
            Statement sentencia = conexion.createStatement();
            ResultSet resul = sentencia.executeQuery(sql);
            while (resul.next()) {
                int ID = resul.getInt(1);
                String NOMBRE = resul.getString(2);
                Date fecha = resul.getDate(3);
                System.out.println("ID: " + ID + ", Apellidos: " + NOMBRE + "FECHA: " + fecha + " => ");
                // extraer columna DIREC TABLA_ANIDADA()
                try {

                    Array personas = (Array) resul.getObject(4);
                    Object[] personasAtributos = (Object[]) personas.getArray();
                    if (personasAtributos.length == 0) {
                        System.out.printf("\tNO TIENE NINGUNA Persona - TABLA ANIDADA VACIA \n");
                    } else {
                        for (int i = 0; i < personasAtributos.length; i++) {
                            try {
                                Struct persona = (Struct) personasAtributos[i];
                                // Saco sus atributos CALLE, CIUDAD, //CODIGO_POST
                                Object[] atributos = persona.getAttributes();

                                java.math.BigDecimal cod = (java.math.BigDecimal) atributos[0];
                                String nombre = (String) atributos[1];

                                Struct direccion = (Struct) atributos[2];

                                Object[] atributos2 = direccion.getAttributes();
                                String calle = (String) atributos2[0];
                                String ciudad = (String) atributos2[1];
                                java.math.BigDecimal codigo_post = (java.math.BigDecimal) atributos2[2];

                                Timestamp fechaNac = (Timestamp) atributos[3];

                                System.out.printf(cod.intValue() + " " + nombre + " " + " " + ciudad + " " + codigo_post.intValue() + " " + fechaNac + "\n");

                            } catch (java.lang.NullPointerException n) {
                                System.out.print("nula ");
                            }
                        }
                        System.out.println();
                    }
                } catch (java.lang.NullPointerException n) {
                    System.out.printf("\tNO TIENE DIRECCIONES - TABLA ANIDADA NULL \n");
                }
            }
            resul.close(); // Cerrar ResultSet
            sentencia.close(); // Cerrar Statement
            conexion.close(); // Cerrar conexión
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }*/
    public static void infoMalaga(Connection con) throws SQLException {
        PreparedStatement consulta = con.prepareStatement("SELECT  G.ID, G.NOMBRE, P.*, G.FECHA_CREACION FROM GRUPOS2 G, TABLE(G.PERSONAS) P WHERE P.DIREC.CIUDAD='Malaga'");
        ResultSet rs = consulta.executeQuery();

        while (rs.next()) {
            int idGrupo = rs.getInt(1);
            String nombreGrupo = rs.getString(2);
            int codPers = rs.getInt(3);  
            String nombre = rs.getString(4); 

            
            Struct direccion = (Struct) rs.getObject(5);
            Object[] dAtrib = direccion.getAttributes();
            String calle = (String) dAtrib[0];
            String ciudad = (String) dAtrib[1];
            int codPostal = ((Number) dAtrib[2]).intValue();

            Timestamp fNac = rs.getTimestamp(6);
            Timestamp fCreacion = rs.getTimestamp(7);

            System.out.println("Id Grupo: " + idGrupo + " | Nombre Grupo: " + nombreGrupo + " | Fecha Creacion: " + fCreacion
                    + " | Codigo Persona: " + codPers + " | Nombre Persona: " + nombre + " | F.Nac: " + fNac
                    + " | Calle: " + calle + " | Ciudad: " + ciudad + " | Cod.Postal: " + codPostal);

        }
    }
    
    public static void ochenteros(Connection con) throws SQLException{
        PreparedStatement consulta = con.prepareStatement("SELECT G.NOMBRE, P.NOMBRE, P.FECHA_NAC FROM GRUPOS2 G, TABLE(G.PERSONAS) P WHERE P.FECHA_NAC < TO_DATE('1990-01-01', 'YYYY-MM-DD')");
        ResultSet rs = consulta.executeQuery();
        
        while(rs.next()){
            String grupo = rs.getString(1);
            String nombrePers = rs.getString(2);
            Timestamp fNac = rs.getTimestamp(3);
            
            System.out.println("Grupo: "+grupo+" | Nombre Persona: "+ nombrePers+ " F.Nac: "+fNac);
        }
                
    }
}
