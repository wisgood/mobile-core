package com.bi.common.dm.pojo;

public class DMURLRule {

	/**
	 * third_id,third_name, second_id, second_name, first_id , first_name, url, flag_id, flag_name
	 * 
	 * @author liuyn
	 * 
	 */
		private int thirdId;
		private String thirdName;
		private int secondId;
		private String secondName;
		private int firstId;
		private String firstName;
		
		private String url;
		private int flagId;
		private String flagName;

		public DMURLRule(int third_id, String third_name, int second_id, String second_name,
				int first_id, String first_name , String url, int flag_id, String flag_name) {			
			super();
			this.thirdId = third_id;
			this.thirdName = third_name;
			this.secondId = second_id;
			this.secondName = second_name;
			this.firstId = first_id;
			this.firstName = first_name;
			
			this.url = url;
			this.flagId = flag_id;
			this.flagName = flag_name;
		}

		public long getThirdId() {
			return thirdId;
		}

		public void setThirdId(int third_id) {
			this.thirdId = third_id;
		}

		public long getSecondId() {
			return secondId;
		}

		public void setSecondId(int second_id) {
			this.secondId = second_id;
		}
        
		public long getFirstId() {
			return firstId;
		}

		public void setFirstId(int first_id) {
			this.firstId = first_id;
		}
		
		public String getThirdName() {
			return thirdName;
		}

		public void setThirdName(String third_name) {
			this.thirdName = third_name;
		}

		public String getSecondName() {
			return secondName;
		}

		public void setSecondName(String second_name) {
			this.secondName = second_name;
		}
        
		public String getFirstName() {
			return firstName;
		}

		public void setFirstName(String first_name) {
			this.firstName = first_name;
		}
		
		
		public String getUrl() {
			return url;
		}

		public void setUrl(String url) {
			this.url = url;
		}

		public int getFlagId() {
			return flagId;
		}

		public void setFlagId(int flag_id) {
			this.flagId = flag_id;
		}

		public String getFlagName() {
			return flagName;
		}

		public void setFlagName(String flag_name) {
			this.flagName = flag_name;
		}

}


