
package RelacionSolo;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Ejercicio3 {
    public static void main(String[] args) {
        try {
            String url = "jdbc:oracle:thin:@localhost:1521/XEPDB1";
            String user = "system";
            String password = "1234";
            Connection con = DriverManager.getConnection(url, user, password);
            
            //insertarDatosCuenta(con, "C/Penelope", "Barcelona", 32910, 90, "Atlas", "1990-02-18", 53450, 0);
            
            //insertarDatosMovimiento(con, 53450, 8000, "R", "2024-05-09");
            
            mostrarInfo(con, 53450);
        } catch (SQLException ex) {
            Logger.getLogger(Ejercicio3.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
    }
    
    public static void insertarDatosCuenta(Connection con, String calle, String ciudad, int codPostal, 
            int codPers, String nombrePers, String fechaNac,
            int numCuenta, double saldo) throws SQLException{
        Struct d1 = con.createStruct("DIRECCION", new Object[]{calle, ciudad, codPostal});
        Struct p1 = con.createStruct("PERSONA", new Object[]{codPers, nombrePers, d1, Date.valueOf(fechaNac)});
        
        PreparedStatement insert = con.prepareStatement("INSERT INTO CUENTAS VALUES(?, ?, ?)");
        insert.setInt(1, numCuenta);
        insert.setObject(2, p1);
        insert.setDouble(3, saldo);
        insert.executeUpdate();
    }
    
    public static void insertarDatosMovimiento(Connection con, int numCuenta, double importe, String tipoMov, String fecha) throws SQLException{
        Struct tipoMovim = con.createStruct("T_MOVIM", new Object[]{importe, tipoMov, Date.valueOf(fecha)});
        
        PreparedStatement insert = con.prepareStatement("INSERT INTO MOVIMIENTOS VALUES(?, ?)");
        insert.setInt(1, numCuenta);
        insert.setObject(2, tipoMovim);
        insert.executeUpdate();
        
    }
    
    public static void mostrarInfo(Connection con, int numCuenta) throws SQLException{
        PreparedStatement consulta = con.prepareStatement("SELECT SUM(M.MOV.IMPORTE) FROM MOVIMIENTOS M WHERE M.NUMCTA = ? AND M.MOV.TIPOMOV = 'I'");
        consulta.setInt(1, numCuenta);
        
        ResultSet rs = consulta.executeQuery();
        int sumaIngresos = 0;
        while(rs.next()){
            sumaIngresos = rs.getInt(1);
        }
        
        System.out.println("Total de ingresos: "+sumaIngresos);
        
        
        
        PreparedStatement consulta2 = con.prepareStatement("SELECT SUM(M.MOV.IMPORTE) FROM MOVIMIENTOS M WHERE M.NUMCTA = ? AND M.MOV.TIPOMOV = 'R'");
        consulta2.setInt(1, numCuenta);
        
        ResultSet rs2 = consulta.executeQuery();
        int sumaReintegros = 0;
        while(rs2.next()){
            sumaReintegros = rs2.getInt(1);
        }
        
        System.out.println("Total de reintegros: "+sumaReintegros);
        
    }
    
}
