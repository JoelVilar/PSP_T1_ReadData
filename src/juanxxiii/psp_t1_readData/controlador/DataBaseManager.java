package juanxxiii.psp_t1_readData.controlador;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import juanxxiii.psp_t1_readData.modelo.ReaderThread;

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
	
	public void readDataByThreads(){
		int count = getCount();
		
		switch(count) {
		case -1:
			System.out.println("Error en la carga de datos");
			break;
		case 0:
			System.out.println("No hay registros para leer.");
			break;
		default:
			try(Connection connection = DriverManager.getConnection(URL)){
				ArrayList<ReaderThread> readers = new ArrayList<>();
				int exceded = count%5;
				int generalCountPerThread= (count-exceded)/5;
				Long timeStart = System.nanoTime();
				for(int x=0;x<5;x++) {
					if(x<4)readers.add(new ReaderThread(generalCountPerThread, generalCountPerThread*x));
					else readers.add(new ReaderThread(generalCountPerThread+exceded, generalCountPerThread*x));
					readers.get(x).run();
				}
				int ingresosTotales=0;
				for(int x=0; x<readers.size();x++) {
					ingresosTotales+=readers.get(x).getIngresosTotales();
				}
				float timeEnd = (float)(System.nanoTime()-timeStart);
				System.out.println("Igresos totales: " + ingresosTotales + ". Calculado en " + timeEnd/1000000000 + "s");
			}catch(SQLException e) {
				System.out.println("Error: " + e.getMessage());
			}
			break;
		}
	}
	
	public int getCount() {
		int count=-1;
		try(Connection connection = DriverManager.getConnection(URL)){
			ResultSet counter = connection.createStatement().executeQuery("SELECT COUNT(ID) FROM EMPLEADOS");
			counter.next();
			count = counter.getInt(1);
		}catch(SQLException e) {
			System.out.println("Error: " + e.getMessage());
		}
		return count;
	}
}
