/**
*
* MODULE FILE NAME: DTORankComparator.java
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
import it.sistematica.spla.datamodel.core.model.ProgrammingRequest;

/**
 * The comparator between PR by rank
 *
 * @author bunkheila
 *
 */
public class PRRankComparator implements Comparator<ProgrammingRequest> {

	/**
	 * Compare DTO by rank.
	 *
	 * @param pR1
	 *            - the first PR
	 * @param pR2
	 *            - the second PR
	 * @return value > 0 if AR of pR1 rank is lower than AR of pR2 rank
	 */
	@Override
	public int compare(ProgrammingRequest pR1, ProgrammingRequest pR2) {

		int compVal = 0;
		
		if (pR1.getAcquisitionRequestList().get(0).getRank() != null 
				&& pR2.getAcquisitionRequestList().get(0).getRank() != null) {
		
			compVal = Integer.compare(pR1.getAcquisitionRequestList().get(0).getRank(), 
					pR2.getAcquisitionRequestList().get(0).getRank());
		}

		return compVal;
	}

}
