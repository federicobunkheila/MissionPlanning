/**
*
* MODULE FILE NAME: PitchIntDetComparator.java
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

import com.nais.spla.brm.library.main.ontology.utils.PitchIntervals;

/**
 * The comparator between Pitch Interval Details by Id.
 *
 * @author bunkheila
 *
 */
public class PitchIntDetComparator implements Comparator<PitchIntervals> {

	/**
	 * Compare Pitch Interval Detail by Id.
	 *
	 * @param pi1
	 *            - the first Pitch Interval Detail
	 * @param pi2
	 *            - the second Pitch Interval Detail
	 * @return value > 0 if pid1 sId is higher than pid2 id
	 */
	@Override
	public int compare(PitchIntervals pi1, PitchIntervals pi2) {

		int compVal = Double.compare(pi1.getIntervalId(), pi2.getIntervalId());

		return compVal;
	}

}
