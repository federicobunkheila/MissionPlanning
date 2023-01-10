/**
*
* MODULE FILE NAME: SessionChecker.java
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

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.TextFormat.ParseException;
import com.telespazio.csg.spla.csps.core.server.Configuration;
import com.telespazio.csg.spla.csps.processor.SessionActivator;
import com.telespazio.splaif.protobuf.ActivateMessage.Activate;

import it.sistematica.spla.datamodel.core.enums.PlanningSessionStatus;
import it.sistematica.spla.datamodel.core.enums.PlanningSessionType;
import it.sistematica.spla.datamodel.core.model.PlanningSession;
import it.sistematica.spla.datamodel.core.model.resource.AcquisitionStation;

/**
 * The session checker.
 *
 * @author bunkheila
 *
 */
public class SessionChecker {

	/**
	 * The proper logger
	 */
	private static Logger logger = LoggerFactory.getLogger(SessionChecker.class);

	/**
	 * Return true if the Planning Session is Delta Plan type, false otherwise
	 *
	 * @param pSId
	 * @return
	 */
	public static boolean isDelta(Long pSId) {

		/**
		 * The DeltaPlan boolean
		 */
		boolean isDeltaPlan = false;

		if (SessionActivator.planSessionMap.get(pSId).getPlanningSessionType().equals(
				PlanningSessionType.VeryUrgent)
			|| SessionActivator.planSessionMap.get(pSId).getPlanningSessionType()
			.equals(PlanningSessionType.LastMinutePlanning)) {

			// Set DeltaPlan
			isDeltaPlan = true;
		}

		return isDeltaPlan;
	}
	
	/**
	 * Return true if the Planning Session is the first of the MH, false otherwise
	 *
	 * @param pSId
	 * @param activate
	 * @return
	 */
	public static boolean isInitial(Long pSId, Activate activate) {

		/**
		 * The First Session boolean
		 */
		boolean isInitial = false;

		if (! activate.hasWorkingPlanningSession()) {

			isInitial = true;
		}

		return isInitial;
	}
	
	/**
	 * Return true if the Planning Session is Delta Plan type, false otherwise
	 *
	 * @param pS
	 * @return
	 */
	public static boolean isDelta(PlanningSession pS) {

		/**
		 * The DeltaPlan boolean
		 */
		boolean isDeltaPlan = false;

		if (pS.getPlanningSessionType().equals(PlanningSessionType.VeryUrgent)
			|| pS.getPlanningSessionType().equals(PlanningSessionType.LastMinutePlanning)) {

			// Set DeltaPlan	
			isDeltaPlan = true;
		}

		return isDeltaPlan;
	}
	
	
	/**
	 * Return true if the Planning Session is UnrankedRoutine type, false
	 * otherwise
	 *
	 * @param pSId
	 * @return
	 */
	public static boolean isUnranked(Long pSId) {

		/**
		 * The Unranked boolean
		 */
		boolean isUnranked = false;

		if (SessionActivator.planSessionMap.get(pSId).getPlanningSessionType()
				.equals(PlanningSessionType.UnrankedRoutine)) {

			// Set Unranked
			isUnranked = true;
		}

		return isUnranked;
	}
	
	/**
	 * Return true if the Planning Session is SelfGenerated type, false
	 * otherwise
	 *
	 * @param pSId
	 * @return
	 */
	public static boolean isSelf(Long pSId) {

		/**
		 * The Self boolean
		 */
		boolean isSelf = false;

		if (SessionActivator.planSessionMap.get(pSId).getPlanningSessionType()
				.equals(PlanningSessionType.SelfGenerated)) {

			// Set Self
			isSelf = true;
		}

		return isSelf;
	}
	
	/**
	 * Return true if the Planning Session is in Final status, false
	 * otherwise
	 *
	 * @param pSId
	 * @return
	 */
	public static boolean isFinal(Long pSId) {

		/**
		 * The Final boolean
		 */
		boolean isFinal = false;

		if (SessionActivator.planSessionMap.get(pSId).getStatus().equals(
						PlanningSessionStatus.Final)) {

			// Set Final
			isFinal = true;
		}

		return isFinal;
	}

	/**
	 * Return true if the session is in the first MH of the day // TODO: check
	 * deprecation and confirm according to UTC date (NTP)
	 *
	 * @param pSId
	 * @throws ParseException
	 */
	@SuppressWarnings("deprecation")
	public static boolean isFirstMH(Long pSId) throws ParseException {

		/**
		 * The first boolean
		 */
		boolean isFirst = false;

		/**
		 * The MH start date
		 */
		Date mhStartDate = SessionActivator.planSessionMap.get(pSId).getMissionHorizonStartTime();
		
		/**
		 * The MH midnight date
		 */
		Date mhMidnightDate = (Date) mhStartDate.clone();
		mhMidnightDate.setHours(0);
		mhMidnightDate.setMinutes(0);
		mhMidnightDate.setSeconds(0);
		
		/**
		 * The first MH date
		 */		
		Date mhFirstDate = new Date(mhMidnightDate.getTime() + (3600 * 6 + 21 * 60) * 1000);
		
		/**
		 * The second MH date
		 */	
		Date mhSecondDate = new Date(mhMidnightDate.getTime() + (3600 * 18 + 21 * 60) * 1000);
		
		if (mhStartDate.compareTo(mhFirstDate) >= 0 && mhStartDate.compareTo(mhSecondDate) < 0) {

			logger.trace("The Planning Session is in the first MH.");
			
			// set First
			isFirst = true;

		} else {

			logger.trace("The Planning Session is in the second MH.");
		}
		
		return isFirst;
	}

	/**
	 * Return true if the Planning Session is Veto enabled, false otherwise
	 *
	 * @param pSId
	 * @return
	 */
	public static boolean isManual(Long pSId) {

		/**
		 * The Manual boolean
		 */	
		boolean isManual = false;

		if (SessionActivator.planSessionMap.get(pSId).getPlanningSessionType()
				.equals(PlanningSessionType.ManualPlanning)) {

			// Set Manual
			isManual = true;
		}

		return isManual;
	}

	
	/**
	 * Return true if the Planning Session is Premium type, false otherwise
	 *
	 * @param pSId
	 * @param partnerId
	 * @return
	 */
	public static boolean isPremium(Long pSId, String partnerId) {

		/**
		 * The Premium boolean
		 */	
		boolean isPremium = false;

		if (SessionChecker.isDefHP(pSId, partnerId) 
				|| SessionChecker.isCivilPP(pSId, partnerId)) {

				// Set PP
			isPremium = true;

		}

		return isPremium;
	}
	
	/**
	 * Return true if the Planning Session is HP type, false otherwise
	 *
	 * @param pSId
	 * @param partnerId
	 * @return
	 */
	public static boolean isDefHP(Long pSId, String partnerId) {

		/**
		 * The PP boolean
		 */	
		boolean isDef = false;

		if (SessionActivator.planSessionMap.get(pSId).getPlanningSessionType().equals(PlanningSessionType.Negotiation)
				|| SessionActivator.planSessionMap.get(pSId).getPlanningSessionType().equals(PlanningSessionType.AllPartnersCheckConflict)
				|| SessionActivator.planSessionMap.get(pSId).getPlanningSessionType().equals(PlanningSessionType.AllPartnersLimitedCheckConflict)
				|| SessionActivator.planSessionMap.get(pSId).getPlanningSessionType().equals(PlanningSessionType.PartnerOnlyCheckConflict)) {

			if (partnerId.startsWith("2")) {

				// Set PP
				isDef = true;
			}
		}

		return isDef;
	}
	
	/**
	 * Return true if the Planning Session is PP type, false otherwise
	 *
	 * @param pSId
	 * @param partnerId
	 * @return
	 */
	public static boolean isCivilPP(Long pSId, String partnerId) {

		/**
		 * The PP boolean
		 */	
		boolean isCivil = false;

		if (SessionActivator.planSessionMap.get(pSId).getPlanningSessionType().equals(PlanningSessionType.Negotiation)
				|| SessionActivator.planSessionMap.get(pSId).getPlanningSessionType().equals(PlanningSessionType.AllPartnersCheckConflict)
				|| SessionActivator.planSessionMap.get(pSId).getPlanningSessionType().equals(PlanningSessionType.AllPartnersLimitedCheckConflict)
				|| SessionActivator.planSessionMap.get(pSId).getPlanningSessionType().equals(PlanningSessionType.PartnerOnlyCheckConflict)) {

			if (partnerId.startsWith("1")) {

				// Set PP
				isCivil = true;
			}
		}

		return isCivil;
	}
	
	/**
	 * Return true if the Planning Session is Ranked Routine type, false otherwise
	 *
	 * @param pSId
	 * @return
	 */
	public static boolean isRankedRoutine(Long pSId) {

		/**
		 * The RankedRoutine boolean
		 */	
		boolean isRankedRoutine = false;

		if (SessionActivator.planSessionMap.get(pSId).getPlanningSessionType()
				.equals(PlanningSessionType.InterCategoryRankedRoutine)
				|| SessionActivator.planSessionMap.get(pSId).getPlanningSessionType()
						.equals(PlanningSessionType.IntraCategoryRankedRoutine)) {
			
			// Set RankedRoutine
			isRankedRoutine = true;
		}

		return isRankedRoutine;
	}

	/**
	 * Return true if the Planning Session is Ranked Routine type, false otherwise
	 *
	 * @param pSId
	 * @return
	 */
	public static boolean isRanked(Long pSId) {

		/**
		 * The Ranked boolean
		 */	
		boolean isRanked = false;

		if (!isManual(pSId) && !isUnranked(pSId) 
				&& !isSelf(pSId) && !isDelta(pSId)) {

			// Set Ranked
			isRanked = true;
		}

		return isRanked;
	}

	/**
	 * Return true if the Planning Session is Routine type, false otherwise
	 *
	 * @param pSId
	 * @return
	 */
	public static boolean isRoutine(Long pSId) {

		/**
		 * The Routine boolean
		 */	
		boolean isRoutine = false;

		if (SessionActivator.planSessionMap.get(pSId).getPlanningSessionType()
				.equals(PlanningSessionType.InterCategoryRankedRoutine)
				|| SessionActivator.planSessionMap.get(pSId).getPlanningSessionType()
						.equals(PlanningSessionType.IntraCategoryRankedRoutine)
				|| isUnranked(pSId) || isSelf(pSId)) {

			// Set Routine
			isRoutine = true;
		}

		return isRoutine;
	}

	/**
	 * Return true if the Planning Session is Subscription enabled, false otherwise
	 *
	 * @param pSId
	 * @return
	 */
	public static boolean isSubscriptionEnabled(Long pSId) {
		
		/**
		 * The Sub boolean
		 */	
		boolean isSub = false;

		if (SessionActivator.planSessionMap.get(pSId).getPlanningSessionType()
				.equals(PlanningSessionType.AllPartnersCheckConflict)
				|| SessionActivator.planSessionMap.get(pSId).getPlanningSessionType()
						.equals(PlanningSessionType.AllPartnersLimitedCheckConflict)
				|| SessionActivator.planSessionMap.get(pSId).getPlanningSessionType()
						.equals(PlanningSessionType.PartnerOnlyCheckConflict)) {

			// Set Sub
			isSub = true;
		}

		return isSub;
	}

	/**
	 * Return true if the Planning Session is Veto enabled, false otherwise
	 * // TODO: test SelfGenerated according to the new flux
	 *
	 * @param pSId
	 * @return
	 */
	public static boolean isVetoEnabled(Long pSId) {

		/**
		 * The Veto boolean
		 */	
		boolean isVeto = true;

		if (Configuration.debugSCMFlag) {

			// Unset Veto
			isVeto = false;

		} else if (SessionActivator.planSessionMap.get(pSId).getPlanningSessionType()
				.equals(PlanningSessionType.AllPartnersCheckConflict)
				|| SessionActivator.planSessionMap.get(pSId).getPlanningSessionType()
						.equals(PlanningSessionType.AllPartnersLimitedCheckConflict)
				|| SessionActivator.planSessionMap.get(pSId).getPlanningSessionType()
						.equals(PlanningSessionType.PartnerOnlyCheckConflict) 
				|| SessionChecker.isSelf(pSId)) { 									  

			// Unset Veto
			isVeto = false;

		} else {

			logger.trace("The Planning Session is enabled for Veto filtering.");
		}

		return isVeto;
	}
	
	/**
	 * Check if two ugs have common external stations 
	 * @param pSId
	 * @param masterUgsId
	 * @param slaveUgsId
	 * @return
	 */
	static public boolean isCommonExtStation(Long pSId, String masterUgsId, String slaveUgsId) {
		
		boolean isCommonExt = false;
		
		String masterOwnerId = SessionActivator.ugsOwnerIdMap.get(pSId).get(masterUgsId);
				
		String slaveOwnerId = SessionActivator.ugsOwnerIdMap.get(pSId).get(slaveUgsId);		
		
		
		for (AcquisitionStation masterAcqStation : 
			SessionActivator.ownerAcqStationListMap.get(pSId).get(masterOwnerId)) {		
			
			for (AcquisitionStation slaveAcqStation : 
				SessionActivator.ownerAcqStationListMap.get(pSId).get(slaveOwnerId)) {
				
				if (masterAcqStation.getCatalogAcquisitionStation().getAcquisitionStationId().equals(
					slaveAcqStation.getCatalogAcquisitionStation().getAcquisitionStationId())
					&& masterAcqStation.getCatalogAcquisitionStation().isExternalStationFlag() 
					&& slaveAcqStation.getCatalogAcquisitionStation().isExternalStationFlag()) {
					
					isCommonExt = true;
					
					break;
				}
			}
		}
		
		return isCommonExt;		
	}
}
