package zara.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;

/**
 * Mejorar cada uno de los métodos a nivel SQL y código cuando sea necesario 
 * Razonar cada una de las mejoras que se han implementado 
 * No es necesario que el código implementado funcione
 */
public class TestSqlDao {

	/**
	 * Obtiene el ID del último pedido para cada usuario
	 * 
	 * no se considera buena práctica la Exception class, mantenido para no romper la compatibilidad 
	 * modificado el procesamiento para dentro de la base de datos 
	 * cerrando resultset y statement
	 * 
	 * @throws Exception
	 * 
	 */
	public Hashtable<Long, Long> getMaxUserOrderId(long idTienda) throws Exception { // TODO: Refatore
		String query = String.format(
				"select max(ID_PEDIDO) as ID_PEDIDO, ID_USUARIO from PEDIDOS where ID_TIENDA = %s group by ID_TIENDA, ID_USUARIO asc ",
				idTienda);

		Hashtable<Long, Long> maxOrderUser = new Hashtable<Long, Long>();

		try (Connection conn = getConnection();
				PreparedStatement stmt = conn.prepareStatement(query);
				ResultSet rs = stmt.executeQuery()) {

			if (null != rs)
				while (rs.next())
					maxOrderUser.put(rs.getLong("ID_USUARIO"), rs.getLong("ID_PEDIDO"));

			if (null != rs)
				rs.close();
			if (null != stmt)
				stmt.close();
			
			// como la conexión no es singleton cierre aquí
			if (null != conn) 
				conn.close();


		} catch (Exception e) {
			System.out.println(e.getMessage());
		}

		return maxOrderUser;
	}

	/**
	 * Copia todos los pedidos de un usuario a otro
	 * 
	 * no se considera buena práctica la Exception class, mantenido para no romper la compatibilidad 
	 * dejando el flujo de inserción para la base de datos (INSERT OR UPDATE QUERY) 
	 * cerrando statement
	 * 
	 * @throws Exception
	 */
	public void copyUserOrders(long idUserOri, long idUserDes) throws Exception { // TODO: Refatore
		String query = String.format(
				"insert into PEDIDOS (FECHA, TOTAL, SUBTOTAL, DIRECCION, ID_USUARIO) "
						+ " select FECHA, TOTAL, SUBTOTAL, DIRECCION, %s from PEDIDOS where ID_USUARIO = %s ",
				idUserDes, idUserOri);

		Connection conn = getConnection();
		try (PreparedStatement stmt = conn.prepareStatement(query)) {
			stmt.executeUpdate();

			conn.setAutoCommit(false);
			conn.commit();

			if (null != stmt)
				stmt.close();
			
			// como la conexión no es singleton cierre aquí
			if (null != conn) 
				conn.close();

		} catch (Exception e) {
			if (conn != null)
				conn.rollback();
			
			// como la conexión no es singleton cierre aquí
			if (null != conn) 
				conn.close();

			System.out.println(e.getMessage());
		}
	}

	/**
	 * Obtiene los datos del usuario y pedido con el pedido de mayor importe para la tienda dada
	 * 
	 * no se considera buena práctica la Exception class, mantenido para no romper la compatibilidad
	 * Java utiliza "pass-by-value", los datos deben ser devueltos por un objeto "cómo no devuelve los datos"
	 * traer el último registro de la base de datos, sin procesamiento para eso
	 * 
	 * @throws Exception
	 * 
	 */
	public void getUserMaxOrder(long idTienda, long userId, long orderId, String name, String address)
			throws Exception { // TODO: Refatore

		String query = String.format("select P.ID_PEDIDO, P.TOTAL, P.ID_USUARIO, U.NOMBRE, U.DIRECCION "
				+ "from PEDIDOS P inner join USUARIOS U on (U.ID_USUARIO = P.ID_USUARIO) " + "where P.ID_TIENDA = %s "
				+ "order by P.TOTAL desc limit 1", idTienda);

		try (Connection conn = getConnection();
				PreparedStatement stmt = conn.prepareStatement(query);
				ResultSet rs = stmt.executeQuery()) {

			//variable para compilar			
			@SuppressWarnings("unused")
			long total = 0;

			if (null != rs)
				if (rs.next()) {
					total = rs.getLong("TOTAL");
					userId = rs.getInt("ID_USUARIO");
					orderId = rs.getInt("ID_PEDIDO");
					name = rs.getString("NOMBRE");
					address = rs.getString("DIRECCION");
					
					System.out.println(address +" >>> Aquí existe, pero no devuelve nada");
				}

			if (null != rs)
				rs.close();
			if (null != stmt)
				stmt.close();
			
			// como la conexión no es singleton cierre aquí
			if (null != conn) 
				conn.close();
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	public Connection getConnection() {
		// para probar siempre devuelve una nueva conexión
		try {
			return DriverManager.getConnection("jdbc:mysql://localhost/contas?useSSL=false&serverTimezone=UTC", "root",
					"root");
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

}
