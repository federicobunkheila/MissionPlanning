/**
*
* MODULE FILE NAME: Partner.java
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

package com.telespazio.csg.spla.csps.model.impl;

import java.util.ArrayList;						   
import com.telespazio.csg.spla.csps.model.IPartner;

/**
 * The Partner class which implements the relevant interface.
 */
public class Partner implements IPartner {

	/**
	 * The Partner Id
	 */
	private String partnerId;
	
	/**
	 * The UGS Id
	 */
	private String ugsId;

	/**
	 * The TUP Id list
	 */
	private ArrayList<String> tupIdList;	
	
	/**
	 * The isDefense
	 */
	private boolean isDefense;
	
	/**
	 * The is Finished boolean
	 */
	private boolean isFinished;

	/**
	 * The Premium BICs
	 */
	private double premBIC;

	/**
	 * The Routine BICs
	 */
	private double routBIC;

	/**
	 * The NEO BICs
	 */
	private double neoBIC;
	
	/**
	 * The Premium BICs at the cat start time
	 */
	private double mhPremBIC;

	/**
	 * The Routine BICs at the cat start time
	 */
	private double mhRoutBIC;

	/**
	 * The NEO BICs at the cat start time
	 */
	private double mhNeoBIC;

	/**
	 * The default constructor
	 */
	public Partner() {

	}

	/**
	 * The filled constructor
	 *
	 * @param partnerId
	 * @param ugsId
	 * @param isFinished
	 */
	public Partner(String partnerId, String ugsId) {
		super();
		this.partnerId = partnerId;
		this.ugsId = ugsId;
	}

	/**
	 * The filled constructor
	 *
	 * @param partnerId
	 * @param ugsId
	 * @param premBIC
	 * @param routBIC
	 * @param neoBIC
	 * @param initPremBIC
	 * @param initRoutBIC
	 * @param initNeoBIC
	 * @param isFinished
	 */
	public Partner(String partnerId, String ugsId, Double premBIC, Double routBIC, Double neoBIC,
			Double mhPremBIC, Double mhRoutBIC, Double mhNeoBIC, boolean isFinished) {
		super();
		this.partnerId = partnerId;
		this.ugsId = ugsId;
		this.isFinished = isFinished;
		this.premBIC = premBIC;
		this.routBIC = routBIC;
		this.neoBIC = neoBIC;
		this.mhPremBIC = mhPremBIC;
		this.mhRoutBIC = mhRoutBIC;
		this.mhNeoBIC = mhNeoBIC;
	}

	/**
	 * @return the partnerId
	 */
	@Override
	public String getId() {
		return this.partnerId;
	}

	/**
	 * @param partnerId the partnerId to set
	 */
	@Override
	public void setId(String partnerId) {
		this.partnerId = partnerId;
	}

	/**
	 * @return the ugsId
	 */
	public String getUgsId() {
		return this.ugsId;
	}

	/**
	 * @param ugsId the ugsId to set
	 */
	public void setUgsId(String ugsId) {
		this.ugsId = ugsId;
	}
	
	/**
	 * @return the tupIdList
	 */
	public ArrayList<String> getTupIdList() {
		return this.tupIdList;
	}

	/**
	 * @param tupIdList the tupIdList to set
	 */
	public void setTUPIdList(ArrayList<String> tupIdList) {
		this.tupIdList = tupIdList;
	}

	/**
	 * @return the isDefense
	 */
	@Override
	public boolean isDefense() {
		return this.isDefense;
	}

	/**
	 * @param isDefense the isDefense to set
	 */
	@Override
	public void setDefense(boolean isDefense) {
		this.isDefense = isDefense;
	}

	/**
	 * @return the premBIC
	 */
	public double getPremBIC() {
		return this.premBIC;
	}

	/**
	 * @param premBIC the premBIC to set
	 */
	public void setPremBIC(double premBIC) {
		this.premBIC = premBIC;
	}

	/**
	 * @return the routBIC
	 */
	public double getRoutBIC() {
		return this.routBIC;
	}

	/**
	 * @param routBIC the routBIC to set
	 */
	public void setRoutBIC(double routBIC) {
		this.routBIC = routBIC;
	}

	/**
	 * @return the neoBIC
	 */
	public double getNeoBIC() {
		return this.neoBIC;
	}

	/**
	 * @param neoBIC the neoBIC to set
	 */
	public void setNeoBIC(double neoBIC) {
		this.neoBIC = neoBIC;
	}
	
	/**
	 * @return the partnerId
	 */
	public String getPartnerId() {
		return partnerId;
	}

	/**
	 * @param partnerId the partnerId to set
	 */
	public void setPartnerId(String partnerId) {
		this.partnerId = partnerId;
	}

	/**
	 * @return the mhPremBIC
	 */
	public double getMHPremBIC() {
		return mhPremBIC;
	}

	/**
	 * @param mhPremBIC the mhPremBIC to set
	 */
	public void setMHPremBIC(double mhPremBIC) {
		this.mhPremBIC = mhPremBIC;
	}

	/**
	 * @return the mhRoutBIC
	 */
	public double getMHRoutBIC() {
		return mhRoutBIC;
	}

	/**
	 * @param mhRoutBIC the mhRoutBIC to set
	 */
	public void setMHRoutBIC(double mhRoutBIC) {
		this.mhRoutBIC = mhRoutBIC;
	}

	/**
	 * @return the mhNeoBIC
	 */
	public double getMHNeoBIC() {
		return mhNeoBIC;
	}

	/**
	 * @param mhNeoBIC the mhNeoBIC to set
	 */
	public void setMHNeoBIC(double mhNeoBIC) {
		this.mhNeoBIC = mhNeoBIC;
	}

	/**
	 * @return the isFinished
	 */
	@Override
	public boolean isFinished() {
		return this.isFinished;
	}

	/**
	 * @param isFinished the isFinished to set
	 */
	@Override
	public void setFinished(boolean isFinished) {
		this.isFinished = isFinished;
	}

}
