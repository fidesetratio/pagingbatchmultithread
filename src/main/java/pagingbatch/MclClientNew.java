package pagingbatch;

public class MclClientNew {

	
	
		private String mcl_id;
		private String mcl_first;

		public MclClientNew() {
			
		}
		public MclClientNew(String mcl_id, String mcl_first) {
				this.mcl_first = mcl_first;
				this.mcl_id = mcl_id;
				
		}

		public String getMcl_id() {
			return mcl_id;
		}
		public void setMcl_id(String mcl_id) {
			this.mcl_id = mcl_id;
		}
		public String getMcl_first() {
			return mcl_first;
		}
		public void setMcl_first(String mcl_first) {
			this.mcl_first = mcl_first;
		}
		
}
