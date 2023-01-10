/**
*
* MODULE FILE NAME: DTOTimeComparator.java
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

import it.sistematica.spla.datamodel.core.model.DTO;

/**
 * The comparator between DTO by start time.
 *
 * @author bunkheila
 *
 */
public class DTOTimeComparator implements Comparator<DTO> {

	/**
	 * Compare DTO by start time.
	 *
	 * @param dto1
	 *            - the first DTO
	 * @param dto2
	 *            - the second DTO
	 * @return value > 0 if DTO1 start time is higher than DTO2 start time
	 */
	@Override
	public int compare(DTO dto1, DTO dto2) {
		/**
		 * The comparing value
		 */
		int compVal = Double.compare(dto1.getStartTime().getTime(), dto2.getStartTime().getTime());

		return compVal;
	}

}
