package com.rayful.bulk.util;


// 디폴트 소팅을 위해서 Comparable 인터페이스를 구현한다.
@SuppressWarnings("unchecked")
public class DataKeySort implements Comparable {
  public String dataKeys;
  public int keyOrder;

   // Constructor
   public DataKeySort(String pDataKey, int pOrder) {
     this.dataKeys = pDataKey;
     this.keyOrder = pOrder;
   }

    // Object의 toString 메소드 overriding.. 객체의 문자적 표현
    public String toString() {
     return dataKeys + "( " + keyOrder + " ) "; 
   }
   
	public String getDataKeys() {
  	return dataKeys;
  }
  
  public int getKeyOrder() {
  	return keyOrder;
  }

   // Comparable 인터페이스를 구현한 클래스에서 반드시 overriding 해야만 하는 비교 메쏘드
   public int compareTo(Object o) { 
     // String의 compareTo 메소드를 호출(사전순서적( lexicographically)으로 비교) 
     return dataKeys.compareTo(((DataKeySort)o).dataKeys); 
   }
}

