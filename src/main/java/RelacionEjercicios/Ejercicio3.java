package RelacionEjercicios;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Timestamp;

public class Ejercicio3 {

    public static void main(String[] args) throws SQLException {
        //Inserta datos en las tablas CUENTAS y MOVIMIENTOS. Asigna el valor 0 al saldo.
        String url = "jdbc:oracle:thin:@localhost:1521/XEPDB1";
        String user = "system";
        String password = "1234";
        Connection con = DriverManager.getConnection(url, user, password);
        
        /*PreparedStatement insertar1 = con.prepareStatement("INSERT INTO CUENTAS VALUES(?, ?, ?)");
        
        Struct direccion1 = con.createStruct("DIRECCION", new Object[]{"Calle Merengue", "Madrid", 41863});
        Struct persona1 = con.createStruct("PERSONA", new Object[]{10, "Paco Bombonero", direccion1, Date.valueOf("1965-04-13")});

        insertar1.setInt(1, 1234);
        insertar1.setObject(2, persona1);
        insertar1.setDouble(3, 16000.00);
        
        insertar1.executeUpdate();*/
        
        /*PreparedStatement insertar2 = con.prepareStatement("INSERT INTO MOVIMIENTOS VALUES(?, ?)");
        
        Struct tipoMovimiento = con.createStruct("T_MOVIM", new Object[]{10000.29, "R", Date.valueOf("2025-02-10")});
        
        insertar2.setInt(1, 1234);
        insertar2.setObject(2, tipoMovimiento);
        
        insertar2.executeUpdate();*/
        
        PreparedStatement leer = con.prepareStatement("Select * from CUENTAS C");
        ResultSet rs1 = leer.executeQuery();
        
        while(rs1.next()){
            int numCuenta = rs1.getInt(1);
            Struct pers = (Struct) rs1.getObject(2);
            Object[] atributosPers1 = pers.getAttributes();
            
            int codigoPers1 = ((Number) atributosPers1[0]).intValue();
            String nombrePers1 = (String) atributosPers1[1];
            
            Struct direcPers1 = (Struct) atributosPers1[2];
            Object[] atributosDirec1 = direcPers1.getAttributes();
            String calle1 = (String) atributosDirec1[0];
            String ciudad1 = (String) atributosDirec1[1];
            int codigoPostal1 = ((Number) atributosDirec1[2]).intValue();
            Timestamp fNac1 = (Timestamp) atributosPers1[3];
            
            double saldo1 = rs1.getDouble(3);
            
            System.out.println("Numero de cuenta: "+numCuenta + " | Saldo: "+saldo1);
            System.out.println("Persona: "+ "Codigo: "+codigoPers1+ " | Nombre: "+nombrePers1+" | "+"F Nac: "+fNac1.toString());
            System.out.println("Direccion: "+ "Calle: "+calle1+ " | Ciudad: "+ciudad1+ " | Codigo Postal: "+codigoPostal1);
            System.out.println("");
        }
        
        PreparedStatement leer2 = con.prepareStatement("SELECT * FROM MOVIMIENTOS");
        ResultSet rs2 = leer2.executeQuery();
        
        while(rs2.next()){
            int numCuenta = rs2.getInt(1);
            Struct tipoMovim = (Struct) rs2.getObject(2);
            Object[] tipoMovimAtrib = tipoMovim.getAttributes();
            BigDecimal importe = (BigDecimal) tipoMovimAtrib[0];
            String tipoMov = (String) tipoMovimAtrib[1];
            Timestamp fechaMov = (Timestamp) tipoMovimAtrib[2];
            
            System.out.println("Movimiento. "+ "Num Cuenta: "+numCuenta+ " | Importe: "+importe+" | Tipo: "+tipoMov+" | Fecha: "+fechaMov);
        }
        
        // Modifica el saldo de la cuenta. Debe contener los ingresos menos los reintegros.
        PreparedStatement rs3 = con.prepareStatement("SELECT M.MOV.IMPORTE FROM MOVIMIENTOS M WHERE M.MOV.TIPOMOV = 'I'");
        ResultSet rsIngresos = rs3.executeQuery();
        double contadorIngresos = 0;
        while(rsIngresos.next()){
            BigDecimal ingresos = (BigDecimal) rsIngresos.getBigDecimal(1);
            contadorIngresos += ingresos.doubleValue();
        }
        
        System.out.println("Total de ingresos = "+contadorIngresos);

        PreparedStatement rs4 = con.prepareStatement("SELECT M.MOV.IMPORTE FROM MOVIMIENTOS M WHERE M.MOV.TIPOMOV = 'R'");
        ResultSet rsReintegros = rs4.executeQuery();
        double contadorReintegros = 0;
        while(rsReintegros.next()){
            BigDecimal reintegros = (BigDecimal) rsReintegros.getBigDecimal(1);
            contadorReintegros += reintegros.doubleValue();
        }
        
        System.out.println("Total de reintegros: "+contadorReintegros);
        
        double totalMovimientos = contadorIngresos - contadorReintegros;
        
        //Actualizar saldo
        PreparedStatement updateSaldo = con.prepareStatement("UPDATE CUENTAS C SET SALDO = ? WHERE C.NUMCTA = 1234");
        updateSaldo.setDouble(1, totalMovimientos);
        updateSaldo.executeUpdate();
        
        PreparedStatement leerSaldo = con.prepareStatement("SELECT C.SALDO FROM CUENTAS C WHERE C.NUMCTA = 1234");
        ResultSet res = leerSaldo.executeQuery();
        res.next();
        BigDecimal saldo = res.getBigDecimal(1);
        
        System.out.println("Saldo: "+ saldo);
        
    }
}
