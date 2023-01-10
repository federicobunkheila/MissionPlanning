/**
*
* MODULE FILE NAME: ISchedAR.java
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

import com.telespazio.csg.spla.csps.model.impl.SchedDTO;

import it.sistematica.spla.datamodel.core.model.EquivalentDTO;

/**
 * The SchedAR Interface.
 */
public interface ISchedAR {
	
	/**
	 * @param acqReqId
	 *            the aRId to set
	 */
	void setARId(String acqReqId);

	/**
	 * @return the ARId
	 */
	String getARId();

	/**
	 * @return the schedDTOList
	 */
	ArrayList<SchedDTO> getDtoList();

	/**
	 * @param schedDTOList
	 *            the schedDTOList to set
	 */
	void setSchedDTOList(ArrayList<SchedDTO> schedDTOList);

	/**
	 * @return the equivalent DTO
	 */
	EquivalentDTO getEquivalentDTO();

	/**
	 * @param eqDTO
	 *            the equivalent DTO to set
	 */
	void setEquivalentDTO(EquivalentDTO eqDTO);

}