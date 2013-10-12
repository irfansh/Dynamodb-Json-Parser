package com.model.data;

import java.util.List;

public class DynamoAttributeType {
	private String s;
	public List sS;
	
	public DynamoAttributeType(String s){
		this.s = s;
	}
	public DynamoAttributeType(List sS){
		this.sS = sS;
	}
	
	public DynamoAttributeType(String s, List sS){
		this.s = s;
		this.sS = sS;
	}
	
	public String getS() {
		return s;
	}

	public void setS(String s) {
		this.s = s;
	}
}
