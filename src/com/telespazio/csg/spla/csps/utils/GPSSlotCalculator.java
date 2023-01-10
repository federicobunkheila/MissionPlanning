/**
 *
 * MODULE FILE NAME: GPSSlotCalculator.java
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

import com.telespazio.csg.spla.csps.processor.SessionScheduler;

import it.sistematica.spla.datamodel.core.model.resource.resourceData.Visibility;

public class GPSSlotCalculator {

	/**
	 * Compute the GPS slots relevant to a given list of visibilities
	 *
	 * @param visList
	 * @return
	 */
	private ArrayList<Long> computeGPSSlots(ArrayList<Visibility> visList) {

		/**
		 * The list of time slots
		 */
		ArrayList<Long> timeSlotList = new ArrayList<>();

		for (int i = 0; i < visList.size(); i++) {

			if (i > 0) {

				// Add slot
				timeSlotList.add(visList.get(i).getVisibilityStopTime().getTime()
						- visList.get(i - 1).getVisibilityStopTime().getTime());
			}
		}

		return timeSlotList;

	}

	/**
	 * Match visibilities with GPS slots 
	 * // TODO: TBC PdM/Ext stations Id and consider PAW interval exclusions
	 *
	 * @param visList
	 */
	public void matchVisGPSSlot(ArrayList<Visibility> visList) {

		/**
		 * The list of GPS visibilities
		 */
		ArrayList<ArrayList<Visibility>> gpsVisList = new ArrayList<>();
		
		/**
		 * The list of GPS visibility counters
		 */
		ArrayList<ArrayList<Long>> gpsVisCounterList = new ArrayList<>();

		gpsVisList.add(new ArrayList<Visibility>());
		gpsVisList.add(new ArrayList<Visibility>());
		gpsVisCounterList.add(new ArrayList<Long>());
		gpsVisCounterList.add(new ArrayList<Long>());

		for (Visibility vis : visList) {

			if (vis.isAllocated()) {

				if (vis.getAcquisitionStationId().contains("PDM") 
						|| vis.getAcquisitionStationId().startsWith("110")) {

					// Add visibility
					gpsVisList.get(0).add(vis);
					gpsVisCounterList.get(0).add(vis.getContactCounter());

				} else if (vis.getAcquisitionStationId().contains("COR")
						|| vis.getAcquisitionStationId().contains("KIR")
						|| vis.getAcquisitionStationId().startsWith("130")) {

					// Add visibility
					gpsVisList.get(1).add(vis);
					gpsVisCounterList.get(1).add(vis.getContactCounter());
				}
			}
		}

		/**
		 * The GPS slot list
		 */
		ArrayList<ArrayList<Long>> gpsSlotList = new ArrayList<>();

		gpsSlotList.add(computeGPSSlots(gpsVisList.get(0)));
		gpsSlotList.add(computeGPSSlots(gpsVisList.get(1)));

		// Associate visibility counters to GPS slots
		for (int i = 0; i < gpsSlotList.size(); i++) {
			
			/**
			 * The slot counter
			 */
			int j = 0;

			if (!gpsSlotList.get(i).isEmpty()) {
				
				for (Long counter : gpsSlotList.get(i)) {
	
					SessionScheduler.visCounterGPSSlotMap.put(counter, gpsSlotList.get(i).get(j));
	
					j++;
				}
			}
		}
	}
}
