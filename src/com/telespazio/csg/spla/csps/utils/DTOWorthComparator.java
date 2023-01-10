/**
*
* MODULE FILE NAME: DTOWorthComparator.java
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
import java.util.Date;

import com.telespazio.csg.spla.csps.model.impl.SchedDTO;

/**
 * The comparator between DTO worth by look side and start time.
 *
 * @author bunkheila
 *
 */
public class DTOWorthComparator implements Comparator<SchedDTO> {

	/**
	 * Compare DTO by worth (look side and start time).
	 *
	 * @param dto1
	 *            - the first DTO
	 * @param dto2
	 *            - the second DTO
	 * @return value > 0 if DTO1 start time is higher than DTO2 start time according
	 *         to their look side
	 */
	@Override
	public int compare(SchedDTO dto1, SchedDTO dto2) {

		/**
		 * The first DTO start time
		 */
		double startTime1 = dto1.getStartTime().getTime();

		if (dto1.getLookSide().equalsIgnoreCase("left")) {

			startTime1 += new Date().getTime();
		}

		/**
		 * The second DTO start time
		 */
		double startTime2 = dto2.getStartTime().getTime();

		if (dto2.getLookSide().equalsIgnoreCase("left")) {

			startTime2 += new Date().getTime();
		}
		
		/**
		 * The comparing value
		 */		
		int compVal = Double.compare(startTime1, startTime2);

		return compVal;
	}
}
