package pagingbatch;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

public class JobCompletionListener implements JobExecutionListener {

	@Override
	public void beforeJob(JobExecution jobExecution) {
		// TODO Auto-generated method stub


        if (jobExecution.getStatus() == BatchStatus.COMPLETED)
        {
            // Log statement
            System.out.println("BATCH JOB COMPLETED SUCCESSFULLY");
        }
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		// TODO Auto-generated method stub
		
	}

}
