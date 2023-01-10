/**
*
* MODULE FILE NAME: BICCalculator.java
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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.TextFormat.ParseException;
import com.nais.spla.brm.library.main.drools.utils.DroolsUtils;
import com.nais.spla.brm.library.main.ontology.enums.TypeOfAcquisition;
import com.nais.spla.brm.library.main.ontology.tasks.Acquisition;
import com.nais.spla.brm.library.main.ontology.tasks.Task;
import com.telespazio.csg.spla.csps.model.impl.Partner;
import com.telespazio.csg.spla.csps.model.impl.SchedDTO;
import com.telespazio.csg.spla.csps.performer.RulesPerformer;
import com.telespazio.csg.spla.csps.processor.PRListProcessor;
import com.telespazio.csg.spla.csps.processor.SessionActivator;
import com.telespazio.csg.spla.csps.processor.SessionScheduler;

import it.sistematica.spla.datamodel.core.enums.PlanningSessionType;
import it.sistematica.spla.datamodel.core.model.resource.Owner;

/**
 *
 * @author bunkheila
 *
 */
public class BICCalculator {

	/**
	 * The proper logger
	 */
	private static Logger logger = LoggerFactory.getLogger(BICCalculator.class);

	/**
	 * Get actual BIC amount for the given DTO
	 *
	 *	// TODO: add theatre contribution
	 *
	 * @param pSId
	 * @param ugsId
	 * @param pRId
	 * @param aRId
	 * @param dtoId
	 * @return
	 */
	public static double getWorkDTOActualBIC(Long pSId, String ugsId, String pRId, String aRId, String dtoId) {

		/**
		 * The DTO actual BIC
		 */
		double dtoActualBIC = 0;

		// Get BICs related to working acquisition Tasks
		for (Task task : RulesPerformer.brmWorkTaskListMap.get(pSId)) {

			if (task instanceof Acquisition) {

				/**
				 * The scheduling DTO Id
				 */
				String schedDTOId = ObjectMapper.parseDMToSchedDTOId(ugsId, pRId, aRId, dtoId);
				
				// Get DTO BICs
				if (((Acquisition) task).getId().contains(schedDTOId)) {
					
					// Manage Di2S master BICs
					if (((Acquisition) task).getDi2sInfo() != null) {
									
						dtoActualBIC += ((Acquisition) task).getImageBIC() 
								* computeDi2SMasterPerc(pSId, task) / 100.0;
						
					} else {
						
						dtoActualBIC += ((Acquisition) task).getImageBIC();
					}
					
					break;
					
				} else if (((Acquisition) task).getDi2sInfo() != null) {

					// Manage DI2S slave BICs
					if (((Acquisition) task).getDi2sInfo().getRelativeSlaveId().equals(schedDTOId)) {

						dtoActualBIC += ((Acquisition) task).getImageBIC() 
								* computeDi2SSlavePerc(pSId, task) / 100.0;						
						
						break;
					}					
				}
			}
		}

		return dtoActualBIC;
	}

	/**
	 * Get actual BIC amount for the given AR
	 *
	 * @param pSId
	 * @param ugsId
	 * @param pRId
	 * @param aRId
	 * @return
	 */
	public static double getWorkARActualBIC(Long pSId, String ugsId, String pRId, String aRId) {

		/**
		 * The AR actual BIC
		 */
		double aRActualBIC = 0;

		// Get BICs related to Acquisition Task
		for (Task task : RulesPerformer.brmWorkTaskListMap.get(pSId)) {

			if (task instanceof Acquisition) {

				/**
				 * The schedAR Id
				 */
				String schedARId = ObjectMapper.parseDMToSchedARId(ugsId, pRId, aRId);
				
				// Get AR BICs
				if (((Acquisition) task).getId().contains(schedARId)) {

					// Manage Di2S master BICs
					if (((Acquisition) task).getDi2sInfo() != null) {
									
						aRActualBIC += ((Acquisition) task).getImageBIC() 
								* computeDi2SMasterPerc(pSId, task) / 100.0;
						
					} else {
						
						aRActualBIC += ((Acquisition) task).getImageBIC();
					}
					
					break;
				
				} else if (((Acquisition) task).getDi2sInfo() != null) {

					// Manage Di2S slave BICs
					if (((Acquisition) task).getDi2sInfo().getRelativeSlaveId().contains(schedARId)) {

						aRActualBIC += ((Acquisition) task).getImageBIC() 
								* computeDi2SSlavePerc(pSId, task) / 100.0;						
						
						break;
					}					
				}
			}
		}

		return aRActualBIC;
	}

	/**
	 * Get actual BIC amount for the given working PR
	 *
	 * @param pSId
	 * @param ugsId
	 * @param pRId
	 * @return
	 */
	public static double getWorkPRActualBIC(Long pSId, String ugsId, String pRId) {

		/**
		 * The PR actual BIC
		 */
		double pRActualBIC = 0;

		// Get BICs related to Acquisition Task
		for (Task task : RulesPerformer.brmWorkTaskListMap.get(pSId)) {

			// Get PR BICs
			if (task instanceof Acquisition) {

				String schedPRId = ObjectMapper.parseDMToSchedPRId(ugsId, pRId);
				
				if (((Acquisition) task).getId().contains(schedPRId)) {

					// Manage DI2S master BICs
					if (((Acquisition) task).getDi2sInfo() != null) {
									
						pRActualBIC += ((Acquisition) task).getImageBIC()
								* computeDi2SMasterPerc(pSId, task) / 100.0;
						
					} else {
						
						pRActualBIC += ((Acquisition) task).getImageBIC();
						
					}
					
				} else if (((Acquisition) task).getDi2sInfo() != null) {

					// Manage DI2S slave BICs
					if (((Acquisition) task).getDi2sInfo().getRelativeSlaveId().contains(schedPRId)) {

						pRActualBIC += ((Acquisition) task).getImageBIC() 
								* computeDi2SSlavePerc(pSId, task) / 100.0;						
					}					
				}
			}
		}

		return pRActualBIC;
	}
	
	/**
	 * Get consumed quota amount for the given UGS
	 *
	 * @param pSId
	 * @param ugsId
	 * @return
	 */
	public static double getConsumedQuota(Long pSId, String ugsId) {

		/**
		 * The UGS actual BIC
		 */
		double ugsActualBIC = 0;

		RulesPerformer.brmOperMap.get(pSId);
		
		/**
		 * The Acquisition list
		 */
		List<Acquisition> acqList = RulesPerformer.brmOperMap.get(pSId).receiveAllAcquisitions(pSId.toString(), 
				RulesPerformer.brmInstanceMap.get(pSId), SessionScheduler.satListMap.get(pSId).get(0)
				.getCatalogSatellite().getSatelliteId());
		
		if (SessionScheduler.satListMap.get(pSId).size() > 1) {
			
			acqList.addAll(RulesPerformer.brmOperMap.get(pSId).receiveAllAcquisitions(pSId.toString(), 
					RulesPerformer.brmInstanceMap.get(pSId), SessionScheduler.satListMap.get(pSId).get(1)
					.getCatalogSatellite().getSatelliteId()));
		}
		
		// Get BICs related to Acquisition Task
		for (Task task : acqList) {

			// Get PR BICs
			if (task instanceof Acquisition) {

				if (((Acquisition) task).getId().contains(ugsId)) {
					
					// Manage Di2S master BICs
					if (((Acquisition) task).getDi2sInfo() != null) {
									
						ugsActualBIC += ugsActualBIC * computeDi2SMasterPerc(pSId, task) / 100.0;						

					} else {
						
						ugsActualBIC += ((Acquisition) task).getImageBIC();
					}
				
				} else if (((Acquisition) task).getDi2sInfo() != null) {

					// Manage Di2S slave BICs
					if (((Acquisition) task).getDi2sInfo().getRelativeSlaveId().startsWith(ugsId)) {
	
						ugsActualBIC += ((Acquisition) task).getImageBIC() 
								* computeDi2SSlavePerc(pSId, task) / 100.0;						
					}
				}
			}
		}

		return ugsActualBIC;
	}

	/**
	 * Write report file
	 *
	 * @param pSId
	 * @param report
	 * @throws IOException
	 */
	public static void writeBICReportFile(Long pSId, Double[][] report) throws IOException {

		// The output directory
		String outFolder = RulesPerformer.brmParamsMap.get(pSId).getResponceFile();
		
		/**
		 * The BIC Report file path
		 */		
		String bicFilePath = outFolder + File.separator + pSId + "-BICReport.txt";
		
		/**
		 * The buffered writer
		 */
		BufferedWriter bw = new BufferedWriter(new FileWriter(bicFilePath));
		
		// Write
		bw.write("BIC Report for session: " + pSId + "\n");
		bw.write("\n");

		for (int i = 0; i < SessionActivator.ownerListMap.get(pSId).size(); i++) {

			String ownerId = SessionActivator.ownerListMap.get(pSId).get(i).getCatalogOwner().getOwnerId();
			bw.write(ownerId + ": " + "\n");
			bw.write(" Consumed Premium BIC: " + report[i][0] + "\n");
			bw.write(" Consumed Ranked BIC: " + report[i][1] + "\n");
			bw.write(" Consumed Unranked BIC: " + report[i][2] + "\n");
			bw.write(" Available Premium BIC: " + report[i][3] + "\n");
			bw.write(" Available Routine BIC: " + report[i][4] + "\n");
			bw.write(" Available NEO BIC: " + report[i][6] + "\n");
			bw.write("\n");

		}

		logger.debug("Results written on file : " + bicFilePath);

		bw.close();
	}
	
	/**
	 * Compute DI2S master BIC percentage
	 * @param pSId
	 * @param task
	 * @return
	 */
	private static double computeDi2SMasterPerc(Long pSId, Task task) {
		
		/**
		 * The master percentage
		 */
		double masterPerc = 0; 
		
		if (((Acquisition) task).getSensorMode().equals(
				TypeOfAcquisition.SPOTLIGHT_1_MSOR)) 
		{
		
			// Set percentage
			masterPerc = RulesPerformer.brmParamsMap.get(pSId).getPercentBicDi2sMasterMSOR();
		} 
		else if (((Acquisition) task).getSensorMode().equals(
					TypeOfAcquisition.SPOTLIGHT_2_MSOS)
				|| ((Acquisition) task).getSensorMode().equals(
					TypeOfAcquisition.SPOTLIGHT_2A))
		{
			// Set percentage
			masterPerc = RulesPerformer.brmParamsMap.get(pSId).getPercentBicDi2sMasterMSOS();
		}
		else if (((Acquisition) task).getSensorMode().equals(
				TypeOfAcquisition.SPOTLIGHT_2_MSJN)) 
		{		
			// Set percentage // TODO: check!
			masterPerc = RulesPerformer.brmParamsMap.get(pSId).getPercentBicDi2sMasterMSOR();
		}
		
		return masterPerc;
	}
	
	/**
	 * Compute DI2S slave BIC percentage
	 * @param pSId
	 * @param task
	 * @return
	 */
	private static double computeDi2SSlavePerc(Long pSId, Task task) {
		
		/**
		 * The slave percentage
		 */
		double slavePerc = 0; 
		
		if (((Acquisition) task).getSensorMode().equals(
				TypeOfAcquisition.SPOTLIGHT_1_MSOR)) 
		{
			// Set percentage				
			slavePerc = RulesPerformer.brmParamsMap.get(pSId).getPercentBicDi2sSlaveMSOR();
		} 
		else if (((Acquisition) task).getSensorMode().equals(
				TypeOfAcquisition.SPOTLIGHT_2_MSOS) 
				|| ((Acquisition) task).getSensorMode().equals(
						TypeOfAcquisition.SPOTLIGHT_2_MSJN))
		{
			// Set percentage
			slavePerc = RulesPerformer.brmParamsMap.get(pSId).getPercentBicDi2sSlaveMSOS();
		}
		
		return slavePerc;
	}
	
	/**
	 * Check if BICs has to decrement // TODO: final check
	 * @param pSId
	 * @param schedDTOId
	 * @return
	 * @throws ParseException 
	 */
	public static boolean isDecrBIC(Long pSId, SchedDTO schedDTO) throws ParseException {
		
		/**
		 * The output boolean
		 */
		boolean isDecrBIC = true;
		
		/**
		 * The scheduling PR Id
		 */
		String schedPRId = ObjectMapper.getSchedPRId(schedDTO.getDTOId());
			
			
		if (SessionActivator.planSessionMap.get(pSId).getPlanningSessionType().equals(
				PlanningSessionType.IntraCategoryRankedRoutine)) {

		
		} else if (PRListProcessor.pRSchedIdMap.get(pSId).containsKey(schedPRId)) {
		
			if ((PRListProcessor.pRSchedIdMap.get(pSId).get(schedPRId).getType().equals(
					it.sistematica.spla.datamodel.core.enums.PRType.VU_HP)
					|| (PRListProcessor.pRSchedIdMap.get(pSId).get(schedPRId).getType().equals(
							it.sistematica.spla.datamodel.core.enums.PRType.VU_PP))
					|| (PRListProcessor.pRSchedIdMap.get(pSId).get(schedPRId).getType().equals(
							it.sistematica.spla.datamodel.core.enums.PRType.LMP_HP))
					|| (PRListProcessor.pRSchedIdMap.get(pSId).get(schedPRId).getType().equals(
							it.sistematica.spla.datamodel.core.enums.PRType.LMP_PP)))
					|| (SessionChecker.isDelta(pSId) && ! SessionChecker.isFirstMH(pSId))
					|| (SessionChecker.isManual(pSId))) { 														  
				
				isDecrBIC = false;	
			}
		}
		
		logger.trace("BICs have to decrement: " + isDecrBIC);
		
		
		return isDecrBIC;
	}

		/**
		 * Compute the actual BICs of a given owner/partner as: {consPremiumBIC,
		 * consRankedBIC, consUnrankedBIC, availPremiumBIC, availRoutineBIC, consNEOBIC,
		 * availNEOBIC} 
		 * // TODO: test the switch of Mission Horizon
		 *
		 * @param pSId
		 * @param partner
		 * @param owner
		 * @return
		 * @throws Exception
		 */
		public static Double[] computeOwnerBICs(Long pSId, Partner partner, Owner owner) throws Exception {
	
			/**
			 * Instance handlers
			 */
			DroolsUtils droolsUtils = new DroolsUtils();
			
			/**
			 * The BRM partner
			 */
			com.nais.spla.brm.library.main.ontology.resources.Partner brmPartner = droolsUtils
					.receivePartnerWithId(partner.getId(),  pSId.toString(), RulesPerformer.brmInstanceMap.get(pSId));
	
			/**
			 * The owner BICs
			 */
			Double[] ownerBICs = new Double[7];
	
			/**
			 * The starting Premium BIC 
			 */
			// TODO: check for Unranked Routine sessions of 2nd MH
			double startPremiumBIC = partner.getMHPremBIC();
	
			/**
			 * The starting Routine BIC
			 */
			double startRoutineBIC = partner.getMHRoutBIC();
			
			/**
			 * The starting NEO BIC
			 */
			double startNeoBIC = partner.getMHNeoBIC();		
			
			// Init BICs
			ownerBICs[0] = 0.0;
			ownerBICs[1] = 0.0;
			ownerBICs[2] = 0.0;	
			ownerBICs[3] = partner.getPremBIC();
			ownerBICs[4] = partner.getRoutBIC();
			ownerBICs[5] = 0.0;		
			ownerBICs[6] = partner.getNeoBIC();
			
			if (SessionChecker.isUnranked(pSId)) {
				
				// Set consumed BICs
				ownerBICs[0] = startPremiumBIC - partner.getPremBIC();
				ownerBICs[1] = startRoutineBIC - partner.getRoutBIC();
				ownerBICs[2] = brmPartner.getUsedBIC() - (partner.getMHRoutBIC() - partner.getRoutBIC());
				ownerBICs[5] = brmPartner.getUsedNeoBic();
						
				if (SessionChecker.isFirstMH(pSId)) {
									
					// Set available BICs
					ownerBICs[3] = partner.getPremBIC();
					ownerBICs[4] = partner.getRoutBIC();
					ownerBICs[6] = partner.getNeoBIC();
					
				} else {
									
					// Set available BICs								
					ownerBICs[3] = owner.getCatalogOwner().getPremiumBIC();
					ownerBICs[4] =  owner.getCatalogOwner().getRoutineBIC();
					ownerBICs[6] =  owner.getCatalogOwner().getNeoBIC();
				}
			
			} else if (SessionChecker.isFinal(pSId)) {
							
				if (SessionChecker.isFirstMH(pSId)) {
					
					// Set consumed BICs
					ownerBICs[0] = startPremiumBIC - partner.getPremBIC();
					ownerBICs[1] = startRoutineBIC - partner.getRoutBIC();
					ownerBICs[2] = 0.0;
					ownerBICs[5] = brmPartner.getUsedNeoBic();
					
					// Set available BICs
					ownerBICs[3] = partner.getPremBIC();
					ownerBICs[4] = partner.getRoutBIC();
					ownerBICs[6] = partner.getNeoBIC();
					
				} else {
			
					if (SessionScheduler.ownerBICRepMap.containsKey(SessionActivator.workPSIdMap.get(pSId))
							&& !SessionScheduler.ownerBICRepMap.get(SessionActivator.workPSIdMap.get(pSId)).isEmpty()) {
						
						ownerBICs = SessionScheduler.ownerBICRepMap.get(
								SessionActivator.workPSIdMap.get(pSId)).get(partner.getId());
						
						if (SessionChecker.isDelta(pSId)) {
							
							SessionScheduler.ownerBICRepMap.put(pSId, 
									SessionScheduler.ownerBICRepMap.get(SessionActivator.workPSIdMap.get(pSId)));
						}
	
					} else {
						
						logger.info("Working Planning Session NOT found for Planning Session: " + pSId);
						logger.info("Default BIC values are set.");
						
						// Set initial Catalog BICs
						ownerBICs[3] = owner.getCatalogOwner().getPremiumBIC();
						ownerBICs[4] = owner.getCatalogOwner().getRoutineBIC();
						ownerBICs[6] = owner.getCatalogOwner().getNeoBIC();
					}	
				}
				
			} else if (SessionChecker.isRankedRoutine(pSId)) {
	
				// Set consumed BICs in MH
				ownerBICs[0] = startPremiumBIC - partner.getPremBIC();
				ownerBICs[1] = brmPartner.getUsedBIC();
				ownerBICs[2] = 0.0;	
				ownerBICs[5] = brmPartner.getUsedNeoBic();			
				// Set available BICs
				ownerBICs[3] = partner.getPremBIC();
				ownerBICs[4] = startRoutineBIC - ownerBICs[1];
				ownerBICs[6] = partner.getNeoBIC();
	
			} else if (SessionChecker.isPremium(pSId, partner.getId())) {
								
				// Set consumed BICs in MH
				ownerBICs[0] = brmPartner.getUsedBIC();
				ownerBICs[1] = 0.0;
				ownerBICs[2] = 0.0;	
				ownerBICs[5] = brmPartner.getUsedNeoBic();
				
				// Set available BICs
				ownerBICs[3] = startPremiumBIC - ownerBICs[0];
				ownerBICs[4] = startRoutineBIC;
				ownerBICs[6] = startNeoBIC - ownerBICs[5];
			}
			
			// Add owners BICs to the map
			SessionScheduler.ownerBICRepMap.get(pSId).put(partner.getId(), ownerBICs);
			
			return ownerBICs;
		}

	/**
	 * Compute the BIC Report of a given owner/UGS as: {consPremiumBIC,
	 * consRankedBIC, consUnrankedBIC, availPremiumBIC, availRoutineBIC, consNEOBIC,
	 * availNEOBIC} 
	 * 
	 * @param pSId
	 * @param partner
	 * @param owner
	 * @return
	 * @throws Exception
	 */
	public static Double[] computeBICReport(Long pSId, Partner partner, Owner owner) throws Exception {
	
		Double[] ownerBICs = BICCalculator.computeOwnerBICs(pSId, partner, owner);
	
		// Add owners BICs to the map
		SessionScheduler.ownerBICRepMap.get(pSId).put(partner.getId(), ownerBICs);
		
		return ownerBICs;
	}
}
