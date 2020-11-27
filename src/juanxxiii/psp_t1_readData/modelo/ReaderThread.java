package juanxxiii.psp_t1_readData.modelo;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Clase que extiende de Thread, la cual calculará los ingresos totales de los registros encontrados
 * en la base de datos dentro de un rango de ids que nosotros le pasemos.
 * @author Atila
 *
 */
public class ReaderThread extends Thread{
	private static final String URL= "jdbc:mysql://localhost/BBDD_PSP_1?serverTimeZone=UTC&user=DAM2020_PSP&password=DAM2020_PSP";
	private static final String SELECT_QUERY_1= "SELECT * FROM EMPLEADOS WHERE (ID>";
	private static final String SELECT_QUERY_2= ") ORDER BY ID LIMIT ";
	private static final String INGRESOS_TAG = "INGRESOS";
	private int ingresosTotales;
	private int employeesToRead;
	private int startingId;
	
	/**
	 * Constructor por parámetros al cual le pasamos los empleados por leer y el id por el que se empieza a 
	 * recoger información.
	 * @param employeesToRead
	 * @param startingId
	 */
	public ReaderThread(int employeesToRead, int startingId) {
		this.ingresosTotales=0;
		this.employeesToRead=employeesToRead;
		this.startingId = startingId;
	}
	
	/**
	 * Método sobreescrito de la interface Runnable, el cual recoge los registros de la base de datos y 
	 * calcula sus ingresos totales.
	 */
	@Override
	public void run() {
		/* Como únicamente utilizaremos un ResultSet, podemos meter este en un try con recursos para que
		 * al finalizar el bloque try este se cierre automáticamente por la interface Closeable, quedándonos
		 * el código más limpio.*/
		try(ResultSet employees = DriverManager.getConnection(URL)
												.createStatement()
												.executeQuery(SELECT_QUERY_1
																+ startingId
																+ SELECT_QUERY_2
																+ employeesToRead)){
			while(employees.next()) {
				ingresosTotales+=employees.getInt(INGRESOS_TAG);
			}
		}catch(SQLException e) {
			System.out.println("Error: " + e.getMessage());
		}
	}
	/**
	 * Devuelve la cantidad de ingresos totales.
	 * @return
	 */
	public int getIngresosTotales() {
		return ingresosTotales;
	}
	/**
	 * Modifica la cantidad de ingresos totales.
	 * @param ingresosTotales
	 */
	public void setIngresosTotales(int ingresosTotales) {
		this.ingresosTotales = ingresosTotales;
	}
	/**
	 * Devuelve la cantidad de empleados que tiene que leer.
	 * @return int.
	 */
	public int getEmployeesToRead() {
		return employeesToRead;
	}
	/**
	 * Modifica la cantidad de empleados que tiene que leer.
	 * @param employeesToRead
	 */
	public void setEmployeesToRead(int employeesToRead) {
		this.employeesToRead = employeesToRead;
	}
	/**
	 * Devuelve el id por el que se comienza a leer.
	 * @return int.
	 */
	public int getStartingId() {
		return startingId;
	}
	/**
	 * Modifica el id por el que se tiene que comenzar a leer.
	 * @param startingId
	 */
	public void setStartingId(int startingId) {
		this.startingId = startingId;
	}
}
