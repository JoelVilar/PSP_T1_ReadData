package juanxxiii.psp_t1_readData.controlador;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import juanxxiii.psp_t1_readData.modelo.Empleado;

public class DataBaseManager {
	private final String URL = "jdbc:mariadb://37.134.98.200:3306/BBDD_PSP_1?serverTimeZone=UTC&user=joel&password=atila95";
	private final String SELECT_ALL_QUERY = "SELECT * FROM EMPLEADOS";
	public DataBaseManager() {
		
	}
	public void readDataSequentially(){
		try(Connection connection = DriverManager.getConnection(URL)){
			Empleado empleado;
			Long timeStart = System.nanoTime();
			ResultSet rs = connection.createStatement().executeQuery(SELECT_ALL_QUERY);
			int ingresosTotales=0;
			int ingresos;
			while(rs.next()) {
				ingresos=rs.getInt("INGRESOS");
				ingresosTotales+=ingresos;
				empleado = new Empleado(rs.getInt("ID"),
										rs.getString("EMAIL"),
										ingresos);
				System.out.println(empleado);
			}
			float timeEnd = (float)(System.nanoTime()-timeStart);
			System.out.println("La suma total de los ingresos es " + ingresosTotales + ", calculado en " + (timeEnd)/1000000000 + " segundos");
		}catch(SQLException e) {
			System.out.println("Error: " + e.getMessage());
		}
	}
}
