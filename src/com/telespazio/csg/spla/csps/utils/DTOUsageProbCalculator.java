/**
*
* MODULE FILE NAME: DTOUsageProbCalculator.java
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

/**
 * The DTO Usage Probability Calculator
 *
 * @author bunkheila
 *
 */
public class DTOUsageProbCalculator {

	/**
	 * Get the probability of DTO usage
	 *
	 * @param dtoSize
	 * @param dtoConflNum
	 * @param aRConflNum
	 * @return
	 */
	public double getDTOUsageProb(double dtoSize, double dtoConflNum, double aRConflNum) {
		
		// Return usage prob
		return (((2 * aRConflNum) / dtoSize) - dtoConflNum) / aRConflNum;
	}

}
