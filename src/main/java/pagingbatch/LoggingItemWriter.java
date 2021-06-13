package pagingbatch;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;

public class LoggingItemWriter implements ItemWriter<MclClientNew>{

	private static final Logger LOGGER = LoggerFactory.getLogger(LoggingItemWriter.class);

	@Override
	public void write(List<? extends MclClientNew> list) throws Exception {
		// TODO Auto-generated method stub
		
		for(MclClientNew cl:list) {
			System.out.println(cl.getMcl_first() +" "+cl.getMcl_id());
		}
		 LOGGER.info("Writing size: {} list = {}", list.size(),list);

	}
	

}
