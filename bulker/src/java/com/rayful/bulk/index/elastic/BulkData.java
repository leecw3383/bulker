package com.rayful.bulk.index.elastic;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

public class BulkData {
	public static final String INDEX_ID = "_id";
	public static final String INDEX_NAME = "_index";
	public static final String UPDATE_DOC = "doc";
	
	public static final String OP_INDEX = "index";
	public static final String OP_DELETE = "delete";
	public static final String OP_CREATE = "create";
	public static final String OP_UPDATE = "update";
	
	private static final Gson GSON =  new Gson();
			
	private String operation;
	private Map<String, String> header = null;
	private Map<String, Object> body = null;
	
	public BulkData(String operation){
		if(!operation.equals(OP_INDEX) 
				&& !operation.equals(OP_DELETE)
				&& !operation.equals(OP_CREATE)
				&& !operation.equals(OP_UPDATE)){
			throw new IllegalArgumentException("Invalid Operaition : " + operation);
		}
		this.operation = operation;
		
		this.header = new HashMap<String, String>(3);
		this.body = new HashMap<String, Object>();
	}
	
	public void setIndexId(String id) {
		this.header.put(INDEX_ID, id);
	}
	
	public void setIndexName(String name) {
		this.header.put(INDEX_NAME, name);
	}
	
	public void addUpdateData(String key, String value) {
		if(this.operation.equals(OP_DELETE)) {
			throw new IllegalArgumentException("Invalid action");
		}
		
		this.body.put(key, value);
	}
	
	public void addUpdateDataObject(String key, Object value) {
		if(this.operation.equals(OP_DELETE)) {
			throw new IllegalArgumentException("Invalid action");
		}
		
		this.body.put(key, value);
	}
	
	public void toJson(PrintStream st) {
		
		//st.println("{\"" + this.operation + "\":" + GSON.toJson(this.header) + "}");
		st.println(String.format("{\"%s\":%s}", this.operation, GSON.toJson(this.header)));
		if(this.body.size() > 0) {
			if(this.operation.equals(OP_UPDATE)) {
				//st.println("{" + UPDATE_DOC + ":" + GSON.toJson(this.body) + "}");
				st.println(String.format("{\"%s\":%s}", UPDATE_DOC, GSON.toJson(this.header)));
			} else {
				st.println(GSON.toJson(this.body));
			}
		}
	}
	
	public void toJson(OutputStream os) throws Exception {
		this.toJson(new PrintStream(os, true, "UTF-8"));
	}
}
