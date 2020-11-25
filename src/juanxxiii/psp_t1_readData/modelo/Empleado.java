package juanxxiii.psp_t1_readData.modelo;

public class Empleado {
	private int id;
	private String email;
	private int ingresos;

	public Empleado() {
		this.id=0;
		this.email=null;
		this.ingresos=-1;
	}
	
	public Empleado(int id, String email, int ingresos) {
		this.id=id;
		this.email=email;
		this.ingresos=ingresos;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public int getIngresos() {
		return ingresos;
	}

	public void setIngresos(int ingresos) {
		this.ingresos = ingresos;
	}

	@Override
	public String toString() {
		return "Empleado-> ID: " + id + " | EMAIL: " + email + " | INGRESOS: " + ingresos;
	}
	
	
}
