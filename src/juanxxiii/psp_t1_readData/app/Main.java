package juanxxiii.psp_t1_readData.app;

import juanxxiii.psp_t1_readData.controlador.DataBaseManager;
/**
 * Clase Main que carga el programa.
 * @author Atila
 *
 */
public class Main {
	/**
	 * Método para iniciar el programa.
	 * @param args
	 */
	public static void main(String[] args) {
		new DataBaseManager().checkIfDBHasData();
	}
}
