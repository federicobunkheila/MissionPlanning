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

import it.sistematica.spla.datamodel.core.model.resource.resourceData.Visibility;

/**
 * The comparator between visibility by start time.
 *
 * @author bunkheila
 *
 */
public class VisTimeComparator implements Comparator<Visibility> {

	/**
	 * Compare DTO by start time.
	 *
	 * @param dto1
	 *            - the first visibility
	 * @param dto2
	 *            - the second visibility
	 * @return value > 0 if visibility1 start time is higher than visibility2 start time
	 */
	@Override
	public int compare(Visibility vis1, Visibility vis2) {
		/**
		 * The comparing value
		 */
		int compVal = Double.compare(vis1.getVisibilityStartTime().getTime(), vis2.getVisibilityStartTime().getTime());

		return compVal;
	}

}

