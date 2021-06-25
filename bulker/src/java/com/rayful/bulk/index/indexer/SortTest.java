package com.rayful.bulk.index.indexer;

import java.util.Arrays;
import java.util.Comparator;

public class SortTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String[][] sr = {
						{"data", "816"},
						{"data", "916"},
						{"data", "10"},
						{"list", "826"},
						{"list", "30"}};

		/*System.out.println("--[정렬 전]-----------------------------------");

		for(int i = 0 ; i < sr.length ; i++) {
			for(int j=0; j < sr[i].length; j++) {
				System.out.println(sr[i][j]);
			}
		}

		System.out.println();

		Arrays.sort(sr,

			new Comparator()
			{	
				public int compare(Object arg0, Object arg1)
				{
					String s1 = (String) arg0;
					String s2 = (String) arg1;
					return s1.length() - s2.length();
				}
			}

		);
	        
		System.out.println("--[정렬 후]-----------------------------------");
	        
		for(int i = 0 ; i < sr.length ; i++) {
			System.out.println(sr[i]);
		}
		
		System.out.println();*/
		
		printArray(sr);
		
		sortArray(sr);
		
		printArray(sr);

	}
	
	public static void printArray(Object[][] arr) 
	{ 
		for( int i = 0; i < arr.length; i++ ) 
		{ 
			for( int j = 0; j < arr[i].length; j++ ) 
				System.out.print(arr[i][j] + "\t"); 
				System.out.println(); 
			} 
		System.out.println(); 
	} 
	
	public static void sortArray(Object[][] arr) 
	{ 
		Arrays.sort(arr, new Comparator<Object[]>() { 
			public int compare(Object[] arr1, Object[] arr2) { 
				if( ((Comparable)arr1[1]).compareTo(arr2[1]) < 0 ) 
					return -1; 
				else
					return 1; 
			} 
		}); 
	} 

}
