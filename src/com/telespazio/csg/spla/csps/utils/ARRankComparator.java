/**
*
* MODULE FILE NAME: ARRankComparator.java
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

import it.sistematica.spla.datamodel.core.model.AcquisitionRequest;

/**
 * The comparator of the ARs by rank
 *
 * @author federico.bunkheila
 *
 */
public class ARRankComparator implements Comparator<AcquisitionRequest> {

	/**
	 * The comparator between ranks of the SM Acquisition Requests.
	 *
	 * @param aR1
	 * @param aR2
	 * @return value > 0 if aR1 rank is higher than aR2 rank
	 */
	@Override
	public int compare(AcquisitionRequest aR1, AcquisitionRequest aR2) {

		int compVal = Double.compare(aR1.getRank(), aR2.getRank());

		return compVal;
	}

}
