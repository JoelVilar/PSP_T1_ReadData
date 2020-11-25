package juanxxiii.psp_t1_readData.modelo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ReaderThread extends Thread{
	private final String URL= "jdbc:mariadb://37.134.98.200:3306/BBDD_PSP_1?serverTimeZone=UTC&user=joel&password=atila95";
	private final String SELECT_QUERY_1= "SELECT * FROM EMPLEADOS WHERE (ID>";
	private final String SELECT_QUERY_2= ") ORDER BY ID LIMIT ";
	private final String INGRESOS_TAG = "INGRESOS";
	private int ingresosTotales;
	private int employeesToRead;
	private int startingId;
	
	public ReaderThread(int employeesToRead, int startingId) {
		this.ingresosTotales=0;
		this.employeesToRead=employeesToRead;
		this.startingId = startingId;
	}
	
	@Override
	public void run() {
		try(Connection connection = DriverManager.getConnection(URL)){
			ResultSet employees = connection.createStatement()
											.executeQuery(SELECT_QUERY_1
															+ startingId
															+ SELECT_QUERY_2
															+ employeesToRead);
			while(employees.next()) {
				ingresosTotales+=employees.getInt(INGRESOS_TAG);
			}
		}catch(SQLException e) {
			System.out.println("Error: " + e.getMessage());
		}
	}
	
	public int getIngresosTotales() {
		return ingresosTotales;
	}

	public void setIngresosTotales(int ingresosTotales) {
		this.ingresosTotales = ingresosTotales;
	}

	public int getEmployeesToRead() {
		return employeesToRead;
	}

	public void setEmployeesToRead(int employeesToRead) {
		this.employeesToRead = employeesToRead;
	}

	public int getStartingId() {
		return startingId;
	}

	public void setStartingId(int startingId) {
		this.startingId = startingId;
	}
}
