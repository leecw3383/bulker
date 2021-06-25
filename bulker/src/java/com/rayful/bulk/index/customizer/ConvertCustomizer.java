/**
 *******************************************************************
 * 파일명 : ConvertCustomizer.java
 * 파일설명 :  공통로직으로 안되는 색인값들을 처리하는 클래스
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/07/12   정충열    최초작성             
 *******************************************************************
*/
package com.rayful.bulk.index.customizer;

import java.util.Map;

import com.rayful.bulk.index.*;
/**
 * 공통로직으로 안되는 색인값들을 처리하는 클래스<p>
 * 특정 색인컬럼의 데이터는 If문등이 필요한 경우가 있는데 
 * 이럴경우 이 클래스를 상속하여 로직을 구현한다.
 * 이 Interface를 implemets하는 클래스는 다음 메서드를 반드시 구현해야 한다.<p>
*/
public interface ConvertCustomizer 
{
	/**s
	* 색인메인로직을 구현한다.
	*/	
	public void convert( String sDataSourceName,
										Map<String, Object> oReaderMap,
										Map<String, Object> oResultMap, 
										ColumnMatching oColumnMatching
	) throws java.sql.SQLException;
										
										
}