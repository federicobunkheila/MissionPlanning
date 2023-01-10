/**
*
* MODULE FILE NAME: ValueComparator.java
*
* MODULE TYPE:      <Class definition>
*
* FUNCTION:         <Functional description of the DDC>
*
* PURPOSE:          <List of SR>
*
* CREATION DATE:    <01-Jan-2017>
*
* AUTHORS:          bunkheila Bunkheila
*
* DESIGN ISSUE:     1.0
*
* INTERFACES:       <prototype and list of input/output parameters>
*
* SUBORDINATES:     <list of functions called by this DDC>
*
* MODIFICATION HISTORY:
*
*             Date          |  Name      |   New ver.     | Description
* --------------------------+------------+----------------+-------------------------------
* <DD-MMM-YYYY>             | <name>     |<Ver>.<Rel>     | <reasons of changes>
* --------------------------+------------+----------------+-------------------------------
*
* PROCESSING
*/

package com.telespazio.csg.spla.csps.utils;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * The comparator between DTO by look side and start time.
 *
 * @author bunkheila
 *
 */
public class ValueComparator implements Comparator<String> {

	Map<String, Integer> base;

	/**
	 * The Value Comparator constructor
	 *
	 * @param worthDTOMap
	 */
	public ValueComparator(HashMap<String, Integer> worthDTOMap) {
		this.base = worthDTOMap;
	}

	/**
	 * Compare map by integer value // Note: this comparator imposes orderings that
	 * are inconsistent with // equals.
	 */
	@Override
	public int compare(String a, String b) {
		
		/**
		 * The comparing value
		 */
		int compInt;
		
		if (this.base.get(a) >= this.base.get(b)) {
			
			// Set value
			compInt = -1;
		} else {
			
			// Set value
			compInt = 1;
		} // returning 0 would merge keys
	
		return compInt;
	}
}
