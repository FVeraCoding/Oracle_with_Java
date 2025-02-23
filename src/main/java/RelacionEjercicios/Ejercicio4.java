package RelacionEjercicios;

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
import oracle.jdbc.OracleConnection;
import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;

public class Ejercicio4 {

    public static void main(String[] args) throws SQLException {
        String url = "jdbc:oracle:thin:@localhost:1521/XEPDB1";
        String user = "system";
        String password = "1234";

        try (Connection con = DriverManager.getConnection(url, user, password)) {

            //4.1 - Inserts
            OracleConnection oraCon = con.unwrap(OracleConnection.class);

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

            ArrayDescriptor descriptor = ArrayDescriptor.createDescriptor("PERSONA_ARRAY", oraCon);
            ARRAY arrayPersonas = new ARRAY(descriptor, oraCon, personasArray);

            PreparedStatement insertar1 = con.prepareStatement("INSERT INTO GRUPOS VALUES (?, ?)");
            insertar1.setString(1, "INFORMATICA");
            insertar1.setArray(2, arrayPersonas);

            insertar1.executeUpdate();

            insertar1.close();

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

            ArrayDescriptor descriptorAdmin = ArrayDescriptor.createDescriptor("PERSONA_ARRAY", oraCon);
            ARRAY arrayPersonasAdmin = new ARRAY(descriptorAdmin, oraCon, personasAdministracion);

            PreparedStatement insertar2 = con.prepareStatement("INSERT INTO GRUPOS VALUES (?, ?)");
            insertar2.setString(1, "ADMINISTRACION");
            insertar2.setArray(2, arrayPersonasAdmin);

            insertar2.executeUpdate();

            insertar2.close();

            //DIRECCION
            Struct d10 = con.createStruct("DIRECCION", new Object[]{"c/Volga", "Sevilla", 15001});
            Struct d11 = con.createStruct("DIRECCION", new Object[]{"c/Tajo", "Malaga", 15001});

            Struct pd1 = con.createStruct("PERSONA", new Object[]{10, "Jose Miguel", d10, Date.valueOf("1979-12-05")});
            Struct pd2 = con.createStruct("PERSONA", new Object[]{11, "Antonia", d11, Date.valueOf("1980-05-17")});

            Object[] personasDireccion = {pd1, pd2};
            ArrayDescriptor descriptorDir = ArrayDescriptor.createDescriptor("PERSONA_ARRAY", oraCon);
            ARRAY arrayPersonasDir = new ARRAY(descriptorDir, oraCon, personasDireccion);

            PreparedStatement insertar3 = con.prepareStatement("INSERT INTO GRUPOS VALUES (?, ?)");
            insertar3.setString(1, "DIRECCION");
            insertar3.setArray(2, arrayPersonasDir);

            insertar3.executeUpdate();

            insertar3.close();

            // READS
            // Muestra las personas que pertenecen al grupo INFORMATICA
            PreparedStatement leerInformatica = con.prepareStatement("SELECT * FROM GRUPOS G WHERE G.NOMBRE = 'INFORMATICA'");
            ResultSet rs1 = leerInformatica.executeQuery();

            while (rs1.next()) {
                String nombre = rs1.getString(1);
                Array personas = rs1.getArray(2);
                Object[] arrayPersonas2 = (Object[]) personas.getArray();

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

            //Muestra toda la información de todos los grupos que hay en la tabla GRUPOS.
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

            //Actualiza el grupo DIRECCION añadiendo una nueva persona
            PreparedStatement consulta = con.prepareStatement("SELECT * FROM GRUPOS G WHERE G.NOMBRE = 'DIRECCION' ");
            ResultSet rs10 = consulta.executeQuery();
            List<Object> nuevasPersonas = new ArrayList<>();

            while (rs10.next()) {
                Array personas = (Array) rs10.getObject(2);
                Object[] pArray = (Object[]) personas.getArray();

                for (Object obj : pArray) {
                    Struct objStruct = (Struct) obj;
                    nuevasPersonas.add(objStruct);
                }
            }
            Struct dNueva = con.createStruct("DIRECCION", new Object[]{"c/Melanina", "Huelva", 11001});
            Struct pNueva = con.createStruct("PERSONA", new Object[]{12, "Azteca", dNueva, Date.valueOf("1988-01-29")});
            nuevasPersonas.add(pNueva);

            Object[] arrayActualizado1 = nuevasPersonas.toArray();
            ArrayDescriptor descriptor3 = ArrayDescriptor.createDescriptor("PERSONA_ARRAY", oraCon);
            ARRAY arrayPersonas3 = new ARRAY(descriptor, oraCon, arrayActualizado1);
            PreparedStatement actualizar = con.prepareStatement("UPDATE GRUPOS G SET G.PERS = ? WHERE G.NOMBRE = 'DIRECCION'");
            actualizar.setArray(1, arrayPersonas3);

            actualizar.executeUpdate();

            
            // Actualizar el grupo ADMINISTRACIÓN eliminando la persona con codigo 8.
            List<Object> adminActualizado = new ArrayList<>();
            PreparedStatement consulta20 = con.prepareStatement("SELECT * FROM GRUPOS G WHERE G.NOMBRE = 'ADMINISTRACION'");
            ResultSet rs60 = consulta20.executeQuery();
            
            while(rs60.next()){
                Array personas = (Array) rs60.getArray(2);
                Object[] pArray = (Object[])personas.getArray();
                
                for(Object p : pArray){
                    Struct pStruct = (Struct) p;
                    Object[] atributos = pStruct.getAttributes();
                    int codigo = ((Number) atributos[0]).intValue();
                    
                    if(codigo != 8){
                        adminActualizado.add(p);
                    }
                }
                
                Object[] pArrayAct = adminActualizado.toArray();
                ArrayDescriptor descriptor20 = ArrayDescriptor.createDescriptor("PERSONA_ARRAY", oraCon);
                ARRAY arrayActualizado2 = new ARRAY(descriptor, oraCon, pArrayAct);
                
                PreparedStatement actualizar2 = con.prepareStatement("UPDATE GRUPOS G SET G.PERS = ? WHERE G.NOMBRE = 'ADMINISTRACION'");
                actualizar2.setArray(1, arrayActualizado2);
                
                actualizar2.executeUpdate();
                
            }        
            
            
            //Borra el Grupo Informática.
            PreparedStatement borrarInfor = con.prepareStatement("DELETE FROM GRUPOS G WHERE G.NOMBRE = 'INFORMATICA'");
            borrarInfor.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
