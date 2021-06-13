package pagingbatch;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.MapJobExplorerFactoryBean;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

@EnableBatchProcessing
@SpringBootApplication
public class MultiThreadPaging implements CommandLineRunner {
	
	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	 @Autowired
	    private JobLauncher jobLauncher;
	 
	 @Autowired
	 private DataSource dataSource;
	 
	 @Autowired
	 @Qualifier("ds2")
	 private DataSource dataSource2;
	 
	 
	 @Autowired
	 private Job job;
	
	
	
	@Primary
    @Bean(name="test2")
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
	@StepScope
	public JdbcPagingItemReader<MclClientNew> pagingItemReader(
			@Value("#{stepExecutionContext['minValue']}") Long minValue,
			@Value("#{stepExecutionContext['maxValue']}") Long maxValue) 
	{
		System.out.println("reading " + minValue + " to " + maxValue);
		if(dataSource == null) {
			System.out.println("datasource is null");
		}

		Map<String, Order> sortKeys = new HashMap<>();
		sortKeys.put("number1", Order.ASCENDING);
		
		SqlPagingQueryProviderFactoryBean queryProvider = new SqlPagingQueryProviderFactoryBean();
		queryProvider.setSelectClause("select rownum as number1,a.mcl_id,a.mcl_first");
		queryProvider.setFromClause("from (select rownum as number1,mcl_id,mcl_first from eka.mst_client_new) a");
		queryProvider.setWhereClause("where a.number1 >= " + minValue + " and number1 < " + maxValue);
		queryProvider.setSortKeys(sortKeys);
		queryProvider.setDataSource(dataSource2);
		
		JdbcPagingItemReaderBuilder<MclClientNew> reader = new JdbcPagingItemReaderBuilder<>();
		try {
			return reader.name("pagingItemReader").dataSource(dataSource2).pageSize(100).queryProvider(queryProvider.getObject()).rowMapper(new NewMclClientIdMapper()).build();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
		}
	
	@Bean
	public OracleColumnPartioner partitioner() 
	{
		OracleColumnPartioner columnRangePartitioner = new OracleColumnPartioner();
		columnRangePartitioner.setDataSource(dataSource);
		
		return columnRangePartitioner;
	}

	// Master
	@Bean
	public Step step1() 
	{
		return stepBuilderFactory.get("step1")
				.partitioner(slaveStep().getName(), partitioner())
				.step(slaveStep())
				.gridSize(8)
				.taskExecutor(new SimpleAsyncTaskExecutor())
				.build();
	}
	// slave step
		@Bean
		public Step slaveStep() 
		{
			return stepBuilderFactory.get("slaveStep")
					.<MclClientNew, MclClientNew>chunk(1000)
					.reader(pagingItemReader(null, null)).processor(processor())
					.writer(customerItemWriter())
					.listener(new CustomChunkListener())
					.build();
		}
	
		@Bean
		public Job job() 
		{
			return jobBuilderFactory.get("job")
					.listener(new JobCompletionListener())
					.start(step1())
					.build();
		}
		
		@Bean 
		public SimpleProcessor processor()
		{
			return new SimpleProcessor();
		}
		
		@Bean
		@StepScope
		public ItemWriter<MclClientNew> customerItemWriter()
		{
			
			
			return new LoggingItemWriter();
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
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String [] newArgs = new String[] {"inputFlatFile=/data/csv/bigtransactions.csv",
		"inputXmlFile=/data/xml/bigtransactions.xml"};

		SpringApplication.run(MultiThreadPaging.class, newArgs);

	}

}
