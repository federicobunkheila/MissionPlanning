package com.telespazio.csg.spla.csps.utils;

/**
*
* MODULE FILE NAME: DwlTimeComparator.java
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
import java.util.Date;

import it.sistematica.spla.datamodel.core.model.task.Download;

/**
 * The comparator between downloads by start time.
 *
 * @author bunkheila
 *
 */
public class DwlOffsetComparator implements Comparator<Download> {

	/**
	 * The comparator between downloads by carrier selection and start time.
	 *
	 * @param dwl1
	 *            - the first task
	 * @param dwl2
	 *            - the second task
	 * @return value > 0 if dwl1 start time is higher than dwl2 start time according 
	 * to their  carrier selection.
	 */
	@Override
	public int compare(Download dwl1, Download dwl2) {

		/**
		 * The dwl1 start time
		 */
		double startTime1 = dwl1.getStartTime().getTime();

		if (dwl1.getCarrierL2Selection()) {

			startTime1 += new Date().getTime();
		}
		
		/**
		 * The dwl2 start time
		 */
		double startTime2 = dwl2.getStartTime().getTime();

		if (dwl2.getCarrierL2Selection()) {

			startTime2 += new Date().getTime();
		}
	
		/**
		 * The comparing value
		 */		
		int compVal = Double.compare(startTime1, startTime2);

		return compVal;
	}
}
