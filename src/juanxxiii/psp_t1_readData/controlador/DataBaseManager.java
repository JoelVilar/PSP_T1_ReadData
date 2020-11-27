package juanxxiii.psp_t1_readData.controlador;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import juanxxiii.psp_t1_readData.modelo.ReaderThread;
/**
 * Clase que se encarga de la gestión de los datos, la cual gestiona los hilos
 * y obtiene información de la base de datos.
 * @author Atila
 *
 */
public class DataBaseManager {

	private static final int ERROR_VALUE = -1;
	private static final String APP_HEAD = "Práctica del 1er Trimestre. Aplicación para lectura de datos.";
	private static final String SELECT_INGRESOS_ID_QUERY = "SELECT INGRESOS FROM EMPLEADOS WHERE ID=?";
	private static final String LECTURA_SAME_RESOURCE_MSG = "Lectura accediendo los hilos a un mismo recurso.";
	private static final int NANO_FACTOR = 1000000000;
	private static final int LAST_IN_THREAD_ARRAY = 4;
	private static final String LECTURA_BLOQUES_MSG = "Lectura pasando bloques de id a los hilos:";
	private static final String COLUMNA_INGRESOS = "INGRESOS";
	private static final int BEGIN_COUNT = 0;
	private static final String LECTURA_SECUENCIAL_MSG = "Lectura de datos de forma secuencial:";
	private static final String ERROR_HEAD_MSG = "Error: ";
	private static final int COLUMN_ONE = 1;
	private static final String SELECT_COUNT_ID_QUERY = "SELECT COUNT(ID) FROM EMPLEADOS";
	private static final String TOTAL_COUNT_MSG = "Total de registros a leer: ";
	private static final String ERROR_NO_DATA_MSG = "No hay registros para leer.";
	private static final String ERROR_ON_LOAD_MSG = "Error en la carga de datos";
	private static final String URL = "jdbc:mysql://localhost/BBDD_PSP_1?serverTimeZone=UTC&user=DAM2020_PSP&password=DAM2020_PSP";
	private static final String SELECT_ALL_QUERY = "SELECT * FROM EMPLEADOS";
	private static final int THREADS_IN_POOL = 5;
	private ExecutorService executor;
	private int ingresosTotales=BEGIN_COUNT;
	
	/**
	 * Constructor por defecto que inicializa la ThreadPool.
	 */
	public DataBaseManager() {
		executor= Executors.newFixedThreadPool(THREADS_IN_POOL);
	}
	
	/**
	 * Método que comprueba si existen datos para leer. En caso de no haber registros,
	 * no poder conectar con la base de datos o ser capaces de leer, nos lo indicará.
	 */
	public void checkIfDBHasData(){
		System.out.println(APP_HEAD);
		
		int count = getCount();
		
		switch(count) {
		case ERROR_VALUE:
			System.out.println(ERROR_ON_LOAD_MSG);
			break;
		case BEGIN_COUNT:
			System.out.println(ERROR_NO_DATA_MSG);
			break;
		default:
			System.out.println(TOTAL_COUNT_MSG + count);
			readDataSequentially();
			readDbWithThreadsByBlocks(count);
			readDbWithThreads(count);
			break;
		}
	}
	
	/**
	 * Método que realiza una conexión a la base de datos y pregunta por el número de
	 * registros que esta tiene.
	 * 
	 * @return int con el número de registros encontrado, -1 si algo falla.
	 */
	public int getCount() {
		int count=ERROR_VALUE;
		try(Connection connection = DriverManager.getConnection(URL)){
			ResultSet counter = connection.createStatement().executeQuery(SELECT_COUNT_ID_QUERY);
			counter.next();
			count = counter.getInt(COLUMN_ONE);
		}catch(SQLException e) {
			System.out.println(ERROR_HEAD_MSG + e.getMessage());
		}
		return count;
	}

	/**
	 * Método para leer registros de una base de datos de manera secuencial y después, suma los
	 * ingresos recogidos para mostrar el total.
	 */
	public void readDataSequentially(){
		System.out.println(LECTURA_SECUENCIAL_MSG);
		try(Connection connection = DriverManager.getConnection(URL)){
			
			/* Recogemos el tiempo exacto en el que empezamos a consultar la información. Más tarde,
			 * lo restaremos al tiempo en el que se encuentre la aplicación para saber cuanto hemos
			 * tardado en completar la ejecución.*/
			Long timeStart = System.nanoTime();
			ResultSet rs = connection.createStatement().executeQuery(SELECT_ALL_QUERY);
			
			/* Siempre que haya un registro para leer, lo recogeremos y sumaremos a los registros totales.*/
			int ingresosTotales=BEGIN_COUNT;
			int ingresos;
			while(rs.next()) {
				ingresos=rs.getInt(COLUMNA_INGRESOS);
				ingresosTotales+=ingresos;
			}
			
			/* Una vez terminado el proceso, mandamos esta información a un método que se encarga de mostrar el
			 * tiempo de ejecución y los resultados, además de marcar el tiempo de finalización.*/
			endTimeAndPrintResult(timeStart, ingresosTotales);
		
		}catch(SQLException e) {
			System.out.println(ERROR_HEAD_MSG + e.getMessage());
		}
	}
	/**
	 * Método que se encarga de coger la información de la base de datos mediante hilos, a los cuales
	 * les pasaremos un rango de ids para que recojan y operen.
	 * @param count
	 */
	private void readDbWithThreadsByBlocks(int count) {
		System.out.println(LECTURA_BLOQUES_MSG);
		try(Connection connection = DriverManager.getConnection(URL)){
			/* Declaramos un ArrayList donde guardaremos los hilos que utilicemos para más tarde operar con ellos.*/
			ArrayList<ReaderThread> readers = new ArrayList<>();
			
			/* Para calcular los rangos, dividimos los registros hallados por el total de hilos a repartir.
			 * En caso de no haber una división exacta, tendremos en cuenta el resto para añadirlo al último
			 * hilo.
			 * */
			int exceded = count%THREADS_IN_POOL;
			int generalCountPerThread= count-exceded/THREADS_IN_POOL;
			
			/* Recogemos el tiempo del sistema para poder calcular más tarde el tiempo de ejecución.*/
			Long timeStart = System.nanoTime();
			
			/* Abrimos un stream de enteros en el rango de los hilos a utilizar. Por cada entero, estimaremos
			 * la cantidad de registros que nuestro hilo tiene que insertar e instanciaremos e iniciamos un objeto
			 * de la clase ReaderThread, a la cual guardaremos en nuestro ArrayList de hilos.*/
			IntStream.range(BEGIN_COUNT, THREADS_IN_POOL)
				.forEach(i -> {
					int totalEmployees = i<LAST_IN_THREAD_ARRAY ? generalCountPerThread : generalCountPerThread+exceded;
					readers.add(new ReaderThread(totalEmployees, generalCountPerThread*i));
					readers.get(i).start();
				});
			
			/* Esperamos a que el último hilo finalice, siendo este el último en ejecutarse y el que más regsitros tiene,
			 * para poder empezar a pedir los datos obtenidos.*/
			readers.get(LAST_IN_THREAD_ARRAY).join();
			
			/* Sumamos los datos obtenidos en cada hilo en nuestra variable, recorriendo el ArrayList de ReaderThread.*/
			int ingresosTotales=BEGIN_COUNT;
			for(ReaderThread r : readers) {
				ingresosTotales+=r.getIngresosTotales();
			}
			
			/* Pasamos el tiempo de inicio y los ingresos a un método que calcula el tiempo de ejecución y muestra por
			 * pantalla los resultados.*/
			endTimeAndPrintResult(timeStart, ingresosTotales);
		}catch(SQLException | InterruptedException e) {
			System.out.println(ERROR_HEAD_MSG + e.getMessage());
		}
	}
	
	/**
	 * Método por el cual leeremos la base de datos con hilos, accediendo estos registro por registro de manera simultanea,
	 * calculando los ingresos.
	 * @param count
	 */
	public void readDbWithThreads(int count) {
		System.out.println(LECTURA_SAME_RESOURCE_MSG);
		/* Declaramos una variable de tipo AtomicInteger, la cual usaremos de contador por sus características propias, la
		 * cual regula el acceso y modificación de su valor.
		 * */
		AtomicInteger contador = new AtomicInteger(count);
		
		/* Recogemos el tiempo del sistema para más tarde calcular el tiempo de ejecución.*/
		Long timeStart = System.nanoTime();
		
		/* Creamos un array de Future en el cual guardaremos nuestros Futures, que contendrán la información recogida por
		 * cada hilo cuando estos finalicen.
		 * */
		Future<Integer> futures[] = new Future[THREADS_IN_POOL];
		
		/* Abrimos un stream de enteros*/
		IntStream.range(BEGIN_COUNT, THREADS_IN_POOL)
			.forEach(i->{
				/* A cada iteración, instanciaremos un futuro el cual obtendrá los datos de un hilo al finalizar el mismo.
				 * Añadimos este futuro a nuestro array de Future para más tarde consultar su información.
				 * */
				Future<Integer> ingreso = executor.submit(()->{
					/* Devolvemos el resultado del método que recoge los ingresos mientras el contador que le cedemos sea
					 * mayor que 0.
					 * */
					return getIngresosWithThread(contador);
				});
				futures[i] =ingreso;
			});
		
		/* Por cada Future del array, pedimos su información con el método get (el cual hace que nuestra aplicación espere a
		 * que ese hilo termine) y sumamos su valor al conteo total.
		 * */
		for (Future<Integer> f: futures) {
			try {
				ingresosTotales+=f.get().intValue();
			} catch (InterruptedException | ExecutionException e) {
				System.out.println(ERROR_HEAD_MSG + e.getMessage());
			}
		}
		
		/* Cerramos nuestra PoolThread.*/
		executor.shutdown();
		
		/* Pasamos el tiempo de inicio de ejecución y los ingresos totales a un método que nos calcula el tiempo total y muestra
		 * los resultados.
		 * */
		endTimeAndPrintResult(timeStart, ingresosTotales);
	}
	
	
	/**
	 * Método que devuelve los ingresos totales recogido de la base de datos mientras el contador que se le suministra sea mayor
	 * que 0.
	 * 
	 * @param contador
	 * @return int con el total de ingresos recogido.
	 */
	private Integer getIngresosWithThread(AtomicInteger contador) {
		int id;
		int totalIngresos=BEGIN_COUNT;
		
		/* Preguntamos al contador si es mayor que 0, y al mismo tiempo decrementamos este en 1, y en caso de ser mayor
		 * obtendremos otro registro.
		 * */
		while((id=contador.getAndDecrement())>BEGIN_COUNT) {
			try(Connection connect = DriverManager.getConnection(URL)){
				PreparedStatement statement = connect.prepareStatement(SELECT_INGRESOS_ID_QUERY);
				statement.setInt(COLUMN_ONE, id);
				ResultSet employee = statement.executeQuery();
				employee.next();
				
				/* Sumamos los ingresos del empleado recogido en el total.*/
				totalIngresos += employee.getInt(COLUMNA_INGRESOS);
			}catch(SQLException e) {
				System.out.println(ERROR_HEAD_MSG + e.getMessage());
			}
		}
		return Integer.valueOf(totalIngresos);
	}

	/**
	 * Método que calcula el tiempo de ejecución y muestra por pantalla los resultados.
	 * @param timeStart
	 * @param ingresosTotales
	 */
	private void endTimeAndPrintResult(Long timeStart, int ingresosTotales) {
		float timeEnd = (float)(System.nanoTime()-timeStart);
		
		System.out.println("La suma total de los ingresos es "
							+ ingresosTotales
							+ ", calculado en "
							+ timeEnd/NANO_FACTOR
							+ " segundos.");
	}
}
