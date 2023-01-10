package com.telespazio.csg.spla.csps.utils;

/**
*
* MODULE FILE NAME: SchedARIdRankComparator.java
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


import java.util.Comparator;
import com.telespazio.csg.spla.csps.performer.RulesPerformer;


/**
 * The comparator between AR by rank.
 *
 * @author bunkheila
 *
 */
public class SchedARIdRankComparator implements Comparator<String> {

	/**
	 * Compare AR by rank.
	 *
	 * @param schedARId1
	 *            - the first AR Id
	 * @param schedARId2
	 *            - the second AR Id
	 * @return value > 0 if aR1 rank is lower than aR2 rank
	 */
	@Override
	public int compare(String schedARId1, String schedARId2) {
		
		int compVal = 0;
		
		if (RulesPerformer.brmARIdRankMap.containsKey(schedARId1) && 
				RulesPerformer.brmARIdRankMap.containsKey(schedARId2)) {
		
			compVal = Integer.compare(RulesPerformer.brmARIdRankMap.get(schedARId1), 
					RulesPerformer.brmARIdRankMap.get(schedARId2));
		}

		return compVal;
	}
}

