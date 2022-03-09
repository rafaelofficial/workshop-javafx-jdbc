package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import db.DB;
import db.DbException;
import model.dao.DepartmentDao;
import model.entities.Department;

public class DepartmentDaoJDBC implements DepartmentDao {
	
	private Connection conn;
	
	public DepartmentDaoJDBC(Connection conn) {
		this.conn = conn;
	}

	@Override
	public void insert(Department obj) {
		PreparedStatement st = null;
		try {
			st = conn.prepareStatement("INSERT INTO department (Name) VALUES (?)", 
					Statement.RETURN_GENERATED_KEYS);
			
			// configura placeholder
			st.setString(1, obj.getName());
			// executa a inserção no banco de dados
			int rowsAffected = st.executeUpdate();
			// testar se uma ou mais linhas foram alteradas
			if (rowsAffected > 0) {
				ResultSet rs = st.getGeneratedKeys();
				if (rs.next()) {
					int id = rs.getInt(1);
					obj.setId(id);
				}
				DB.closeResultSet(rs);
			}			
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public void update(Department obj) {
		PreparedStatement st = null;
		try {
			st = conn.prepareStatement("UPDATE department SET Name = ? WHERE Id = ?");
			// configura placeholder
			st.setString(1, obj.getName());
			st.setInt(2, obj.getId());
			// executa update no banco de dados
			st.executeUpdate();
			
		} catch (SQLException e) {
			DB.closeStatement(st);
		}
	}

	@Override
	public void deleteById(Integer id) {
		PreparedStatement st = null;
		try {
			st = conn.prepareStatement("DELETE FROM department WHERE Id = ?");
			// configura placeholder
			st.setInt(1, id);
			
			// executa o comando delete no banco de dados
			int rows = st.executeUpdate();
			if (rows == 0) throw new DbException("Id not exist!");
			
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public Department findById(Integer id) {
		PreparedStatement st = null;
		ResultSet rs = null;
		
		try {
			// preparar a consulta sql
			st = conn.prepareStatement("SELECT * FROM department WHERE Id = ?");
			
			// configurar placeholder
			st.setInt(1, id);
			
			// executa query
			rs = st.executeQuery();
			
			// testar se veio um resultado (rs.next)
			if (rs.next()) {
				Department dep = instantiateDepartment(rs);
				return dep;
			}
			return null;
			
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
	}

		// instanciar objeto Department
		private Department instantiateDepartment(ResultSet rs) throws SQLException {
			Department dep = new Department();
			dep.setId(rs.getInt("Id"));
			dep.setName(rs.getString("Name"));
			return dep;
		}

	@Override
	public List<Department> findAll() {
		PreparedStatement st = null;
		ResultSet rs = null;
		
		try {
			// preparar a consulta sql
			st = conn.prepareStatement("SELECT * FROM department");

			// executa query
			rs = st.executeQuery();
			
			// instanciar lista de departamentos
			List<Department> list = new ArrayList<>();
			Map<Integer, Department> map = new HashMap<>();
			
			// testar se veio um resultado (rs.next)
			while (rs.next()) {
				Department dep = map.get(rs.getInt("Id"));
				
				if (dep == null) {
					dep = instantiateDepartment(rs);
					map.put(rs.getInt("Id"), dep);
				}
				list.add(dep);		
			}
			return list;
			
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
	}

}
