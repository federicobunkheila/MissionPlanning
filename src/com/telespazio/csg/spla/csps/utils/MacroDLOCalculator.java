/**
*
* MODULE FILE NAME: MacroDLOCalculator.java
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.telespazio.csg.spla.csps.model.impl.MacroDLO;
import com.telespazio.csg.spla.csps.processor.SessionScheduler;

import it.sistematica.spla.datamodel.core.model.resource.Satellite;
import it.sistematica.spla.datamodel.core.model.resource.resourceData.Visibility;




/**
 * The calculator of the MacroDLO
 *
 * @author bunkheila
 *
 */
public class MacroDLOCalculator {

	/**
	 * The proper logger
	 */
	private static Logger logger = LoggerFactory.getLogger(MacroDLOCalculator.class);

	
	/**
	 * Build the MacroDLOs relevant to the working MH
	 * @param pSId
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static void buildMHMacroDLOs(Long pSId) {

		logger.info("Build the Macro DLO in the MH for download purposes for: " + pSId);
		
		/**
		 * The list of macro DLOs
		 */
		ArrayList<MacroDLO> macroDLOList = new ArrayList<>();

		for (Satellite sat : SessionScheduler.satListMap.get(pSId)) {

			/**
			 * The list of satellite visibilities
			 */
			List<Visibility> satVisList = (ArrayList<Visibility>) sat.getVisibilityList();
			
			if (satVisList == null) {
				
				satVisList = new ArrayList<Visibility>();
			}
			
			// Sort Visibilities
			if (!satVisList.isEmpty()) {			
				Collections.sort(satVisList, new VisTimeComparator());
			}
				
			for (Visibility vis : satVisList) {
			
				/**
				 * The new macro flag
				 */
				boolean isNewMacro = true;

				for (int i = 0; i < macroDLOList.size(); i++) {

					if (sat.getCatalogSatellite().getSatelliteId().equals(macroDLOList.get(i).getSatId()) 
							
							&& (((vis.getVisibilityStartTime().compareTo(macroDLOList.get(i).getStartTime()) >= 0)
									&& (vis.getVisibilityStartTime().compareTo(macroDLOList.get(i).getStopTime()) <= 0))
							
							|| ((vis.getVisibilityStopTime().compareTo(macroDLOList.get(i).getStartTime()) >= 0)
									&& (vis.getVisibilityStopTime().compareTo(macroDLOList.get(i).getStopTime()) <= 0))
							
							|| ((vis.getVisibilityStartTime().compareTo(macroDLOList.get(i).getStartTime()) <= 0)
									&& (vis.getVisibilityStopTime().compareTo(macroDLOList.get(i).getStopTime()) >= 0))
							
							|| ((vis.getVisibilityStartTime().compareTo(macroDLOList.get(i).getStartTime()) >= 0)
									&& (vis.getVisibilityStopTime().compareTo(macroDLOList.get(i).getStopTime()) <= 0)))) {

						isNewMacro = false;

						macroDLOList.get(i).getVisList().add(vis);

						if (vis.getVisibilityStartTime().compareTo(macroDLOList.get(i).getStartTime()) < 0) {

							macroDLOList.get(i).setStartTime(vis.getVisibilityStartTime());
						}

						if (vis.getVisibilityStopTime().compareTo(macroDLOList.get(i).getStopTime()) > 0) {

							macroDLOList.get(i).setStopTime(vis.getVisibilityStopTime());
						}

						macroDLOList.get(i).getVisList().add(vis);

						break;
					}
				}

				if (isNewMacro) {

					/**
					 * The list of visibilities
					 */
					ArrayList<Visibility> visList = new ArrayList<>();

					visList.add(vis);

					/**
					 * The macro DLO
					 */
					MacroDLO macroDLO = new MacroDLO(Long.toString(pSId) + Integer.toString(macroDLOList.size()),
							sat.getCatalogSatellite().getSatelliteId(), vis.getVisibilityStartTime(),
							vis.getVisibilityStopTime(), visList);

					macroDLO.setPacketSeqId(0);
					
					macroDLOList.add(macroDLO);				
				}
			}
		}

		// Sort Macro DLOs
		if (!macroDLOList.isEmpty()) {
			Collections.sort(macroDLOList, new MacroDLOTimeComparator());
		}
		
		// Recompute internal DLOs
		for (int i = 0; i < macroDLOList.size(); i++) {
			
			MacroDLO macroDLO = macroDLOList.get(i);
			
			if (i > 0 && macroDLO.getSatId().equals(macroDLOList.get(i - 1).getSatId())
					&& macroDLO.getStartTime().before(macroDLOList.get(i - 1).getStopTime())) {

				if (macroDLO.getStopTime().compareTo(macroDLOList.get(i - 1).getStopTime()) > 0) {
					
					macroDLOList.get(i - 1).setStopTime(macroDLO.getStopTime());
				}
			
				macroDLOList.remove(i);
				
				i --;
			}
		}

		// Check internal DLOs
		for (int i = 0; i < macroDLOList.size(); i++) {
			
			logger.info ("Computed Macro DLO between: " + macroDLOList.get(i).getStartTime().toString() 
				+ " and " + macroDLOList.get(i).getStopTime().toString() 
				+ " for satellite: " + macroDLOList.get(i).getSatId());
		}
		
		SessionScheduler.macroDLOListMap.put(pSId, (ArrayList<MacroDLO>) macroDLOList.clone());
	}

}
