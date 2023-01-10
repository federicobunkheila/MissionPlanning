/**
*
* MODULE FILE NAME: SchedAR.java
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

import java.io.Serializable;
import java.util.ArrayList;

import com.telespazio.csg.spla.csps.model.ISchedAR;

import it.sistematica.spla.datamodel.core.enums.AcquisitionRequestStatus;
import it.sistematica.spla.datamodel.core.enums.PRMode;
import it.sistematica.spla.datamodel.core.enums.PRType;
import it.sistematica.spla.datamodel.core.model.EquivalentDTO;

/**
 * The SchedAR class which implements the relevant interface.
 */
@SuppressWarnings("serial")
public class SchedAR implements ISchedAR, Serializable {

	/**
	 * The AR Id
	 */
	private String aRId;
	/**
	 * The AR status
	 */
	private AcquisitionRequestStatus aRStatus;
	/**
	 * The scheduling DTO List
	 */
	private ArrayList<SchedDTO> schedDTOList;

	/**
	 * The type of the related PR
	 */
	private PRType pRType;

	/**
	 * The mode of the related PR
	 */
	private PRMode pRMode;

	/**
	 * The equivalent DTO
	 */
	private EquivalentDTO eqDTO;

	/**
	 * The AR relative rank
	 */
	private int relRank;

	/**
	 * The pitch Extra Bic
	 */
	private double pitchExtraBic;

	/**
	 * The default constructor
	 */
	public SchedAR() {

	}

	/**
	 * The filled constructor
	 *
	 * @param ugsId
	 * @param pRId
	 * @param aRId
	 * @param acqReqStatus
	 * @param schedDTOList
	 * @param pRType
	 * @param pRMode
	 */
	public SchedAR(String aRId, AcquisitionRequestStatus acqReqStatus, ArrayList<SchedDTO> schedDTOList, PRType pRType,
			PRMode pRMode) {
		super();
		// this.ugsId = ugsId;
		// this.pRId = pRId;
		this.aRId = aRId;
		this.aRStatus = acqReqStatus;
		this.schedDTOList = schedDTOList;
		this.pRType = pRType;
		this.pRMode = pRMode;
	}

	/**
	 * @return the aRId
	 */
	@Override
	public String getARId() {
		return this.aRId;
	}

	/**
	 * @param aRId
	 *            the aRId to set
	 */
	@Override
	public void setARId(String aRId) {
		this.aRId = aRId;
	}

	/**
	 * @return the acqReqStatus
	 */
	public AcquisitionRequestStatus getARStatus() {
		return this.aRStatus;
	}

	/**
	 * @param acqReqStatus
	 *            the acqReqStatus to set
	 */
	public void setARStatus(AcquisitionRequestStatus acqReqStatus) {
		this.aRStatus = acqReqStatus;
	}

	/**
	 * @return the pRType
	 */
	public PRType getPRType() {
		return this.pRType;
	}

	/**
	 * @param pRType
	 *            the pRType to set
	 */
	public void setPRType(PRType pRType) {
		this.pRType = pRType;
	}

	/**
	 * @return the pRMode
	 */
	public PRMode getPRMode() {
		return this.pRMode;
	}

	/**
	 * @param pRMode
	 *            the pRMode to set
	 */
	public void setPRMode(PRMode pRMode) {
		this.pRMode = pRMode;
	}

	/**
	 * @return the schedDTOList
	 */
	@Override
	public ArrayList<SchedDTO> getDtoList() {
		return this.schedDTOList;
	}

	/**
	 * @param schedDTOList
	 *            the schedDTOList to set
	 */
	@Override
	public void setSchedDTOList(ArrayList<SchedDTO> schedDTOList) {
		this.schedDTOList = schedDTOList;
	}

	/**
	 * @return the eqDTO
	 */
	@Override
	public EquivalentDTO getEquivalentDTO() {
		return this.eqDTO;
	}

	/**
	 * @param eqDTO
	 *            the eqDTO to set
	 */
	@Override
	public void setEquivalentDTO(EquivalentDTO eqDTO) {
		this.eqDTO = eqDTO;
	}

	/**
	 * @return the relRank
	 */
	public int getRelRank() {
		return this.relRank;
	}

	/**
	 * @param relRank
	 *            the relRank to set
	 */
	public void setRelRank(int relRank) {
		this.relRank = relRank;
	}

	/**
	 * @return the pitchExtraBic
	 */
	public double getPitchExtraBic() {
		return pitchExtraBic;
	}

	/**
	 * @param pitchExtraBic
	 *            the pitchExtraBic to set
	 */
	public void setPitchExtraBic(double pitchExtraBic) {
		this.pitchExtraBic = pitchExtraBic;
	}

	/**
	 * Clone the SchedAR data
	 */
	@Override
	@SuppressWarnings("unchecked")
	public SchedAR clone() {

		SchedAR newAR = new SchedAR();

		// newAR.setUgsId(this.ugsId);
		// newAR.setPRId(this.pRId);
		newAR.setARId(this.aRId);
		newAR.setARStatus(this.aRStatus);
		newAR.setPRType(this.pRType);
		newAR.setSchedDTOList((ArrayList<SchedDTO>) this.schedDTOList.clone());
		newAR.setEquivalentDTO(this.eqDTO);
		newAR.setRelRank(this.relRank);
		newAR.setPitchExtraBic(this.pitchExtraBic);

		return newAR;

	}
}
