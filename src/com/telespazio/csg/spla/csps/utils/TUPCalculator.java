package com.telespazio.csg.spla.csps.utils;

import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.sistematica.spla.datamodel.core.model.resource.Satellite;

import com.telespazio.csg.spla.csps.core.server.Configuration;
import com.telespazio.csg.spla.csps.processor.SessionActivator;
import com.telespazio.csg.spla.csps.processor.SessionScheduler;

import it.sistematica.spla.datamodel.core.model.resource.AcquisitionStation;
import it.sistematica.spla.datamodel.core.model.resource.Owner;
import it.sistematica.spla.datamodel.core.model.resource.catalog.CatalogUgs;
import it.sistematica.spla.datamodel.core.model.resource.catalog.UnAvailabilityStatus;
import it.sistematica.spla.datamodel.core.model.resource.resourceData.Visibility;
import it.sistematica.spla.datamodel.core.model.resource.catalog.CatalogAcquisitionStation;

public class TUPCalculator {

	/**
	 * The proper logger
	 */
    private static Logger logger = LoggerFactory.getLogger(TUPCalculator.class);

	
	/**
	 * Check UGS TUP
	 * 
	 * @param pSId
	 * @param ugsId
	 * @return
	 */

	public static boolean isUgsTUP(Long pSId, String ugsId) {
		
		/**
		 * The output boolean
		 */
		boolean isTUP = false;
		
		if (SessionActivator.ugsIsTUPMap.get(pSId).containsKey(ugsId)
			&& SessionActivator.ugsIsTUPMap.get(pSId).get(ugsId)) {
			
			isTUP = true;
		}
		
		return isTUP;
	}
		
	/**
	 * Check TUP activation
	 * 
	 * @param pSId
	 * @param ugsId
	 * @return
	 */
	public boolean isActiveTUP(Long pSId, String ugsId) {
		
		boolean isActive = false;
		
		for (Owner owner : SessionActivator.ownerListMap.get(pSId)) {

			for (CatalogUgs ugsCat : owner.getCatalogOwner().getUgsList()) {

				if (ugsCat.getUgsId().equals(ugsId) && isUgsTUP(pSId, ugsId)
						&& ugsCat.getIsTupActive()) {
				
					isActive = true;
					
					logger.debug("S-TUP relevant to UGS Id: " + ugsId + " is active.");
					
					break;
				}
			}
		}
		
		return isActive;		
	}

	/**
	 * Check TUP station validity
	 * 
	 * @param pSId
	 * @param ugsId
	 * @param acqStationIdList
	 * @return
	 */
	public boolean isValidTUPStation(Long pSId, String ugsId, List<String> acqStationIdList)  {
		
		/**
		 * The output boolean
		 */
		boolean isValid = false;
		
		for (Owner owner : SessionActivator.ownerListMap.get(pSId)) {

			for (CatalogUgs ugsCat : owner.getCatalogOwner().getUgsList()) {

				if (ugsCat.getUgsId().equals(ugsId) && isUgsTUP(pSId, ugsId)
						&& ugsCat.getIsTupActive()) {
					
					for (CatalogAcquisitionStation acqStat : ugsCat.getAcquisitionStationList()) {
							
						for (String acqStationId : acqStationIdList) {
							
							if (acqStat.getAcquisitionStationId().equals(acqStationId)) {
								
								isValid =  true;
								
								logger.debug("S-TUP station " + acqStationId
										+ " relevant to UGS Id: " + ugsId + " is valid.");
								
								break;
							}
						}
						
						if (isValid) {							
							
							break;
						}
					}				
				}
				
				if (isValid) {					
					break;
				}
			}
			
			if (isValid) {			
				break;
			}
		}
		
		return isValid;
		
	}
	
	
	/**
	 * Check TUP availability
	 * 
	 * @param pSId
	 * @param ugsId
	 * @return
	 */
	public boolean isAvailableTUP(Long pSId, String ugsId) {
		
		/**
		 * The output boolean
		 */
		boolean isAvail = false;
		
		for (Owner owner : SessionActivator.ownerListMap.get(pSId)) {

			for (CatalogUgs ugsCat : owner.getCatalogOwner().getUgsList()) {

				if (ugsCat.getUgsId().equals(ugsId) && isUgsTUP(pSId, ugsId)
						&& ugsCat.getIsTupActive()) {
					
					for (CatalogAcquisitionStation acqStat : ugsCat.getAcquisitionStationList()) {
						
						// TODO: check if almost a visibility for the UGS is available in the MH
						for (Satellite sat : SessionScheduler.satListMap.get(pSId))	{
							
							for (Visibility vis : sat.getVisibilityList()) {
								
								if (vis.getAcquisitionStationId().equals(acqStat.getAcquisitionStationId())
										&& isVisInsideTUPMH(pSId, vis)) {
									
									isAvail =  true;
									
									logger.debug("S-TUP relevant to UGS Id: " + ugsId + " is available.");
									
									break;
								}
							}
						}
						
						if (isAvail) {							
							break;
						}
					}
				}
				
				if (isAvail) {					
					break;
				}
			}
			
			if (isAvail) {			
				break;
			}
		}
		
		return isAvail;
		
	}
	
	/**
	 * Check if the visibility is inside the number of Mission Horizons available for the S-TUP 
	 * from the given Planning Session Id
	 * @param pSId
	 * @param vis
	 */
	private static boolean isVisInsideTUPMH(Long pSId, Visibility vis) {
		
		/**
		 * The inside boolean 
		 */
		boolean isInside = true;
		
		if ((vis.getVisibilityStartTime().before(SessionActivator.planSessionMap
				.get(pSId).getMissionHorizonStartTime()))
				|| (vis.getVisibilityStartTime().getTime() > SessionActivator.planSessionMap
				.get(pSId).getMissionHorizonStartTime().getTime() + Configuration.stupMHNumber * 8640000)) {
			
			// Set inside
			isInside = false;
		}
		
		return isInside;
	}
	
	/**
	 * Check available TUP visibility
	 * 
	 * @param pSId
	 * @param vis
	 * @return
	 */
	public boolean isAvailableTUPVis(Long pSId, Visibility vis, AcquisitionStation visAcqStation) {
		
		/**
		 * The output boolean
		 */
		boolean isAvail = true;	
		
//		for (Owner owner : SessionActivator.ownerListMap.get(pSId)) {
//
//			for (CatalogUgs ugsCat : owner.getCatalogOwner().getUgsList()) {
//	
//				logger.debug("Check unavailabilites for UGS: " + ugsCat.getUgsId());
				
		for (UnAvailabilityStatus unavStatus : visAcqStation.getCatalogAcquisitionStation().getUnAvailabilityStatusList()) {
			
			if (unavStatus.getAcqStationId().equals(
					visAcqStation.getCatalogAcquisitionStation().getAcquisitionStationId())) {
			
				if (!unavStatus.getIs_available()
						&& !checkVisWithUnavTime(vis, unavStatus)) {
					
					isAvail = false;
					
					logger.info("Unavailability found for Acquisition Station: " 
							+ unavStatus.getAcqStationId()
							+ " between " + unavStatus.getUnavailability_starttime()
							+ " and " + unavStatus.getUnavailability_stop_time());
					
					break;
											
				} else if (unavStatus.getIs_available()){
					
					logger.debug("Availability found for Acquisition Station: " 
					+ unavStatus.getAcqStationId()
					+ " between " + unavStatus.getUnavailability_starttime() 
					+ " and " + unavStatus.getUnavailability_stop_time());
				}
			}
		}
		
		return isAvail;
	}
	
	/**
	 * Check the visibility wrt a given unavailability time window
	 * 
	 * @param vis
	 * @param unavStatus
	 * @return
	 */
	private boolean checkVisWithUnavTime(Visibility vis, UnAvailabilityStatus unavStatus) {

		logger.debug("Check unavailability for Acquisition Station: " + unavStatus.getAcqStationId()
		+ " between start time: " + unavStatus.getUnavailability_starttime() 
		+ " and stop time: " + unavStatus.getUnavailability_stop_time());		
		/**
		 * The availability boolean
		 */
		boolean isAvail = true;

		if (unavStatus.getUnavailability_starttime() != null) {
		
			/**
			 * The unavailability window dates
			 */
			Date unavStartTime = unavStatus.getUnavailability_starttime();
			Date unavStopTime = unavStatus.getUnavailability_stop_time();
	
			if (unavStopTime == null) {
	
				unavStopTime = new Date((long) Double.POSITIVE_INFINITY);
			}
	
			if ((vis.getVisibilityStartTime().after(unavStartTime)
					&& vis.getVisibilityStartTime().before(unavStopTime))
					|| (vis.getVisibilityStopTime().after(unavStartTime)
						&& vis.getVisibilityStopTime().before(unavStopTime))) {
	
				isAvail = false;
			}
		}

		return isAvail;
	}
}
