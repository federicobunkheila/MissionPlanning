/**
*
* MODULE FILE NAME: SchedDTOPriorityComparator.java
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

import com.telespazio.csg.spla.csps.model.impl.SchedDTO;

/**
 * The comparator between DTO by priority.
 *
 * @author bunkheila
 *
 */
public class SchedDTOPriorityComparator implements Comparator<SchedDTO> {

	/**
	 * Compare PR by priority.
	 *
	 * @param dto1
	 *            - the first DTO
	 * @param dto2
	 *            - the second DTO
	 * @return value > 0 if dto1 priority is lower than dto2 priority
	 */
	@Override
	public int compare(SchedDTO dto1, SchedDTO dto2) {
		
		int compVal = Integer.compare(dto1.getPriority(), dto2.getPriority());

		return compVal;
	}

}
