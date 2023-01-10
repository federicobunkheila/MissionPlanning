/**
*
* MODULE FILE NAME: IPartner.java
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

package com.telespazio.csg.spla.csps.model;

/**
 * The Partner Interface.
 */
public interface IPartner {

	/**
	 * @return the partnerId
	 */
	public abstract String getId();

	/**
	 * @param partnerId
	 *            the partnerId to set
	 */
	public abstract void setId(String partnerId);

	// /**
	// * @return the pSBICMap
	// */
	// public abstract HashMap<PlanningSessionType, Double> getPSBICMap();
	//
	// /**
	// * @param pSBICMap the sessionBICMap to set
	// */
	// public abstract void setPSBICMap(HashMap<PlanningSessionType, Double>
	// pSBICMap);

	/**
	 * @return the isDefense
	 */
	public abstract boolean isDefense();

	/**
	 * @param isDefense
	 *            the isDefense to set
	 */
	public abstract void setDefense(boolean isDefense);

	/**
	 * @return the isFinished
	 */
	public abstract boolean isFinished();

	/**
	 * @param isFinished
	 *            the isFinished to set
	 */
	public abstract void setFinished(boolean isFinished);

}