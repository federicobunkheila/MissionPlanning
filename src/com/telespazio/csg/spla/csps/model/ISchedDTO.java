/**
*
* MODULE FILE NAME: ISchedDTO.java
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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import it.sistematica.spla.datamodel.core.enums.DTOSensorMode;
import it.sistematica.spla.datamodel.core.enums.DtoStatus;
import it.sistematica.spla.datamodel.core.enums.PRMode;
import it.sistematica.spla.datamodel.core.model.UserInfo;

/**
 * The SchedDTO Interface.
 */
public interface ISchedDTO {

	/**
	 * @return the dtoId
	 */
	String getDTOId();

	/**
	 * @param dtoId
	 *            the dtoId to set
	 */
	void setDTOId(String dtoId);

	/**
	 * @return the equivDTOId
	 */
	String getEquivDTOId();

	/**
	 * @param equivDTOId
	 *            the equivDTOId to set
	 */
	void setEquivDTOId(String equivDTOId);
	
	/**
	 * @return the dtoId
	 */
	String getARId();

	/**
	 * @param aRId
	 *            the aRId to set
	 */
	void setARId(String aRId);

	/**
	 * @return the pRId
	 */
	String getPRId();

	/**
	 * @param pRId
	 *            the pRId to set
	 */
	void setPRId(String pRId);

	/**
	 * @return the startTime
	 */
	Date getStartTime();

	/**
	 * @param startTime
	 *            the startTime to set
	 */
	void setStartTime(Date startTime);

	/**
	 * @return the stopTime
	 */
	Date getStopTime();

	/**
	 * @param stopTime
	 *            the stopTime to set
	 */
	void setStopTime(Date stopTime);

	/**
	 * @return the lookSide
	 */
	String getLookSide();

	/**
	 * @param lookSide
	 *            the lookSide to set
	 */
	void setLookSide(String lookSide);

	/**
	 * @return the satelliteId
	 */
	String getSatelliteId();

	/**
	 * @param satelliteId
	 *            the satelliteId to set
	 */
	void setSatelliteId(String satelliteId);

	/**
	 * @return the BIC
	 */
	double getBIC();

	/**
	 * @param bic
	 *            the BIC to set
	 */
	void setBIC(double bic);

	/**
	 * @return the sensorMode
	 */
	DTOSensorMode getSensorMode();

	/**
	 * @param sensorMode
	 *            the sensorMode to set
	 */
	void setSensorMode(DTOSensorMode sensorMode);

	/**
	 * @return the sizeH
	 */
	Double getSizeH();

	/**
	 * @param sizeH
	 *            the sizeH to set
	 */
	void setSizeH(Double sizeH);

	/**
	 * @return the sizeV
	 */
	Double getSizeV();

	/**
	 * @param sizeV
	 *            the sizeV to set
	 */
	void setSizeV(Double sizeV);

	/**
	 * @return the status
	 */
	DtoStatus getStatus();

	/**
	 * @param status
	 *            the status to set
	 */
	void setStatus(DtoStatus status);

	/**
	 * @return the beamId
	 */
	String getBeamId();

	/**
	 * @param beamId
	 *            the beamId to set
	 */
	void setBeamId(String beamId);

	/**
	 * @return the pRMode
	 */
	PRMode getPRMode();

	/**
	 * @param beamId
	 *            the pRMode to set
	 */
	void setPRMode(PRMode pRMode);

	/**
	 * @return the userInfoList
	 */
	List<UserInfo> getUserInfoList();

	/**
	 * @param userInfo
	 *            the userInfo to set
	 */
	void setUserInfoList(List<UserInfo> userInfo);
	
	/**
	 * @return the replDTOIdList
	 */
	List<String> getReplDTOIdList();

	/**
	 * @param replDTOIdList
	 *            the replDTOIdList to set
	 */
	void setReplDTOIdList(ArrayList<String> replDTOIdList);

}
