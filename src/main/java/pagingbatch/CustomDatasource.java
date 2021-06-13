package pagingbatch;

import javax.sql.DataSource;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CustomDatasource {

	@ConfigurationProperties(prefix = "app.datasource")
	@Bean(name="ds2")
		public DataSource dataSource() {
	    	    return DataSourceBuilder
	        .create()
	        .build();
	}
}

