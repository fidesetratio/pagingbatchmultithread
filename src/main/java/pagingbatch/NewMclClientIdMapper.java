package pagingbatch;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

public class NewMclClientIdMapper implements RowMapper<MclClientNew>  {

	@Override
	public MclClientNew mapRow(ResultSet rs, int rowNum) throws SQLException {
		// TODO Auto-generated method stub
	MclClientNew mcl_client_new = new MclClientNew(rs.getString("mcl_id"),rs.getString("mcl_first"));
	return mcl_client_new;
}

}
