/**
*
* MODULE FILE NAME: DLOTimeComparator.java
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

import com.telespazio.csg.spla.csps.model.impl.MacroDLO;


/**
 * The comparator between DLO by start time.
 *
 * @author bunkheila
 *
 */
public class MacroDLOTimeComparator implements Comparator<MacroDLO> {

	/**
	 * Compare DLO by start time.
	 *
	 * @param dlo1
	 *            - the first DLO
	 * @param dlo2
	 *            - the second DLO
	 * @return value > 0 if DLO1 start time is higher than DLO2 start time
	 */
	@Override
	public int compare(MacroDLO dlo1, MacroDLO dlo2) {
		/**
		 * The comparing value
		 */
		int compVal = Double.compare(dlo1.getStartTime().getTime(), dlo2.getStartTime().getTime());

		return compVal;
	}

}
