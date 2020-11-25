package juanxxiii.psp_t1_readData.app;

import juanxxiii.psp_t1_readData.controlador.DataBaseManager;

public class Main {

	public static void main(String[] args) {
		DataBaseManager dbManager = new DataBaseManager();
		dbManager.readDataSequentially();
		dbManager.readDataByThreads();
	}

}
