package pagingbatch;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.MapJobExplorerFactoryBean;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.RowMapper;

//@EnableBatchProcessing
//@SpringBootApplication
public class MainPaging implements CommandLineRunner {
	
	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	 @Autowired
	    private JobLauncher jobLauncher;
	 
	 @Autowired
	    private Job job;
	
	
	
	@Primary
    @Bean(name="test")
    DefaultBatchConfigurer batchConfigurer() {
    return new DefaultBatchConfigurer() {

        private JobRepository jobRepository;
        private JobExplorer jobExplorer;
        private JobLauncher jobLauncher;

        {
            MapJobRepositoryFactoryBean jobRepositoryFactory = new MapJobRepositoryFactoryBean();
            try {
                this.jobRepository = jobRepositoryFactory.getObject();
                MapJobExplorerFactoryBean jobExplorerFactory = new MapJobExplorerFactoryBean(jobRepositoryFactory);
                this.jobExplorer = jobExplorerFactory.getObject();
                SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
                jobLauncher.setJobRepository(jobRepository);
                jobLauncher.afterPropertiesSet();
                this.jobLauncher = jobLauncher;

            } catch (Exception e) {
            }
        }

        @Override
        public JobRepository getJobRepository() {
            return jobRepository;
        }

        @Override
        public JobExplorer getJobExplorer() {
            return jobExplorer;
        }

        @Override
        public JobLauncher getJobLauncher() {
            return jobLauncher;
        }
    };
   }
	
	
	 @Bean
	    public Job jdbcPaginationJob(@Qualifier("jdbcPaginationStep") Step exampleJobStep,
	                                 JobBuilderFactory jobBuilderFactory) {
	        Job databaseCursorJob = jobBuilderFactory.get("jdbcPaginationJob")
	                .incrementer(new RunIdIncrementer())
	                .listener(listener())
	                .flow(exampleJobStep)
	                
	                .end()
	                .build();
	        return databaseCursorJob;
	    }
	
	
	
	 @Bean
	    public JobExecutionListener listener()
	    {
	        return new JobCompletionListener();
	    }
	
	 @Bean
	    public ChunkListener chunkListener()
	    {
	        return new CustomChunkListener();
	    }
	
	
	
	@Bean
 public Step jdbcPaginationStep(@Qualifier("jdbcPaginationItemReader") ItemReader<MclClientNew> reader,
                                @Qualifier("jdbcPaginationItemWriter") ItemWriter<MclClientNew> writer,
                                StepBuilderFactory stepBuilderFactory) {
     return stepBuilderFactory.get("jdbcPaginationStep")
             .<MclClientNew, MclClientNew>chunk(500)
             .reader(reader)
             .writer(writer)
             .listener(chunkListener())
             .build();
 }
	

	

	@Bean
    public ItemReader<MclClientNew> jdbcPaginationItemReader(DataSource dataSource, PagingQueryProvider queryProvider) {
        return new JdbcPagingItemReaderBuilder<MclClientNew>()
                .name("pagingItemReader")
                .dataSource(dataSource)
                .pageSize(100)
                .queryProvider(queryProvider)
                .rowMapper(new RowMapper<MclClientNew>() {
					
					@Override
					public MclClientNew mapRow(ResultSet rs, int rowNum) throws SQLException {
						// TODO Auto-generated method stub
						int columnNumber=rs.getMetaData().getColumnCount();
						ResultSetMetaData metaData = rs.getMetaData();
						int count = metaData.getColumnCount();
						//for (int i = 1; i <= count; i++)
						//{
						    
					//		System.out.println(metaData.getColumnName(i));
						
					//	}
						MclClientNew mcl_client_new = new MclClientNew(rs.getString("mcl_id"),rs.getString("mcl_first"));
						return mcl_client_new;
					}
				})
                .build();
    }

	
	  @Bean
	    public ItemWriter<MclClientNew> jdbcPaginationItemWriter() {
	        return new LoggingItemWriter();
	    }

	
	  
		
	  @Bean
	    public SqlPagingQueryProviderFactoryBean queryProvider(DataSource dataSource) {
	        SqlPagingQueryProviderFactoryBean queryProvider = new SqlPagingQueryProviderFactoryBean();
	        queryProvider.setDataSource(dataSource);
	        StringBuffer buffer = new StringBuffer();
	        buffer.append("select rownum as number1,a.mcl_id,a.mcl_first");
	        queryProvider.setSelectClause(buffer.toString());
	        buffer = new StringBuffer();
	        buffer.append("from eka.mst_client_new a");
	        queryProvider.setFromClause(buffer.toString());
	  	    buffer =new StringBuffer();
	  	    buffer.append("1=1");
	  	    queryProvider.setWhereClause(buffer.toString());
	        queryProvider.setSortKeys(sortByRegSpaj());
	        return queryProvider;
	    }
	
	  
	  
	  private Map<String, Order> sortByRegSpaj() {
	        Map<String, Order> sortConfiguration = new HashMap<>();
	        sortConfiguration.put("number1", Order.DESCENDING);
	        return sortConfiguration;
	    }
	  
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String [] newArgs = new String[] {"inputFlatFile=/data/csv/bigtransactions.csv",
		"inputXmlFile=/data/xml/bigtransactions.xml"};

		SpringApplication.run(MainPaging.class, newArgs);

	}


	@Override
	public void run(String... args) throws Exception {
		// TODO Auto-generated method stub
		JobParameters jobParameters = new JobParametersBuilder()
                .addString("JobId", String.valueOf(System.currentTimeMillis()))
                .addDate("date", new Date())
                .addLong("time",System.currentTimeMillis()).toJobParameters();
         
        JobExecution execution = jobLauncher.run(job, jobParameters);
 
        System.out.println("STATUS :: "+execution.getStatus());
	}

}
