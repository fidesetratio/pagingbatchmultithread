package pagingbatch;

import org.springframework.batch.item.ItemProcessor;

public class SimpleProcessor implements ItemProcessor<MclClientNew, MclClientNew>{

	@Override
	public MclClientNew process(MclClientNew item) throws Exception {
		// TODO Auto-generated method stub
		System.out.println("you need process it here"+item.getMcl_first()+" "+item.getMcl_id());
		return item;
	}

}
