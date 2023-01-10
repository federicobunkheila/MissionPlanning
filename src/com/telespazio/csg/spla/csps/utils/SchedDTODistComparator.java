/**
*
* MODULE FILE NAME: DTODistComparator.java
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
 * The comparator between SM ARs by satellite to target distance.
 *
 * @author bunkheila
 *
 */
public class SchedDTODistComparator implements Comparator<SchedDTO> {

	/**
	 * Compare ARs by satellite to target distance as absolute look angle.
	 *
	 * @param dto1
	 *            - the first DTO
	 * @param dto2
	 *            - the second DTO
	 * @return value > 0 if DTO1 target distance is higher than DTO2 target distance
	 */
	@Override
	public int compare(SchedDTO dto1, SchedDTO dto2) {

		int compVal = Double.compare(Math.abs(dto1.getTargetDistance()), Math.abs(dto2.getTargetDistance()));

		return compVal;
	}

}
