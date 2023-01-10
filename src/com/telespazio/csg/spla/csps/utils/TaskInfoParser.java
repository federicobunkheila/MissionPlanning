/**
*
* MODULE FILE NAME: TaskInfoParser.java
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

import java.math.BigDecimal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nais.spla.brm.library.main.ontology.enums.Polarization;
import com.telespazio.csg.spla.csps.handler.EquivDTOHandler;
import com.telespazio.csg.spla.csps.processor.PRListProcessor;

import it.sistematica.spla.datamodel.core.model.Subswath;
import it.sistematica.spla.datamodel.core.model.task.Acquisition;
import it.sistematica.spla.datamodel.core.model.task.BITE;
import it.sistematica.spla.datamodel.core.model.task.Download;
import it.sistematica.spla.datamodel.core.model.task.Maneuver;
import it.sistematica.spla.datamodel.core.model.task.PassThrough;
import it.sistematica.spla.datamodel.core.model.task.Store;

/**
 * The parser of the additional Task Info (SPARC, Encryption, ...)
 */
public class TaskInfoParser {

	
	/**
	 * The proper logger
	 */
	private static Logger logger = LoggerFactory.getLogger(TaskInfoParser.class);

	
	// /**
	// * The proper logger
	// */
	// private static Logger logger = LoggerFactory
	// .getLogger(PersistPerformer.class);

	/**
	 * Associate info to the acquisition task according to SPARC output
	 * Add lack Info for the Acquisition	
	 *
	 *
	 * @param acq
	 * @param pSId
	 */
	public static void setAcqInfo(Acquisition acq, Long pSId) throws Exception {

		/**
		 * The scheduling DTO Id
		 */
		String schedDTOId = ObjectMapper.parseDMToSchedDTOId(acq.getUgsId(), acq.getProgrammingRequestId(),
				acq.getAcquisitionRequestId(), acq.getDtoId());

		if (PRListProcessor.dtoSchedIdMap.get(pSId).containsKey(schedDTOId)) {

			// TODO: Added on 02/12/2019
			// TODO: Removed polarization on 23/03/2022
//			acq.setPolarization(PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getPolarization());
			acq.setRxPolarization(PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getRxPolarization());
			acq.setTxPolarization(PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getTxPolarization());
			acq.setMultipolarization(PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getMultiPolarization());			
			
			acq.setAncillaryCalibrationBegin(
					PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getAncillaryCalibrationBegin());
			acq.setAncillaryCalibrationEnd(
					PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getAncillaryCalibrationEnd());
			acq.setAncillaryDataBegin(PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getAncillaryDataBegin());
			acq.setAncillaryDataEnd(PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getAncillaryDataEnd());
			acq.setAncillaryNotchBegin(
					PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getAncillaryNotchBegin());
			acq.setAncillaryNotchEnd(PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getAncillaryNotchEnd());
			acq.setAzimuthBeamAdjustment0(
					PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getAzimuthBeamAdjustment0());
			acq.setAzimuthBeamAdjustment1(
					PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getAzimuthBeamAdjustment1());
			acq.setAzimuthBeamAdjustment2(
					PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getAzimuthBeamAdjustment2());
			acq.setAzimuthBeamAdjustment3(
					PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getAzimuthBeamAdjustment3());
			acq.setAzimuthBeamAdjustment4(
					PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getAzimuthBeamAdjustment4());
			acq.setAzimuthBeamAdjustment5(
					PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getAzimuthBeamAdjustment5());
			if (PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getAzimuthBeamStep() != null) {
				acq.setAzimuthBeamStep(Double.parseDouble(Integer
						.toString(PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getAzimuthBeamStep())));
			}
			acq.setCompensationSelection(
					PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getCompensationSelection());
			if (PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getCompressionSettingCalibration() != null) {
				acq.setCompressionSettingCalibration(
						PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getCompressionSettingCalibration());
			}
			acq.setCompressionSettingEcho(
					PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getCompressionSettingEcho());
			acq.setCompressionSettingNoise(
					PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getCompressionSettingNoise());
			acq.setCompressionSettingSampleNumber(
					PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getCompressionSettingSampleNumber());
			acq.setDeltaTimeStart(PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getDeltaTimeStart());
			acq.setDerampingFactor(PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getDerampingFactor());
			if (PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getDi2sCycleLength() != null) {
				acq.setDi2sCycleLength(PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getDi2sCycleLength());
			}
			acq.setDi2sPulseRepetitionInterval(
					PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getDi2sPulseRepetitionInterval());
			acq.setDopplerResidual(PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getDopplerResidual());
			acq.setElevationBeamAdjustment1(
					PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getElevationBeamAdjustment1());
			acq.setElevationBeamAdjustment2(
					PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getElevationBeamAdjustment2());
			if (PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getElevationBeamStep() != null) {
				acq.setElevationBeamStep(Double.parseDouble(Integer
						.toString(PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getElevationBeamStep())));
			}
			acq.setElevationTilt(PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getElevationTilt());
			acq.setFrequencyCompensationIdentifier(
					PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getFrequencyCompensationIdentifier());
			acq.setFrequencyCompensationOffset(
					PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getFrequencyCompensationOffset());
			acq.setImageLenght(PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getImageLength());
			acq.setPanelId(PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getPanelId());
			acq.setSamplingWindowStartTimeAdjustment1(
					PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getSamplingWindowStartTimeAdjustment1());
			acq.setSamplingWindowStartTimeAdjustment2(
					PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getSamplingWindowStartTimeAdjustment2());
			acq.setSamplingWindowStartTimeAdjustment3(
					PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getSamplingWindowStartTimeAdjustment3());
			acq.setSamplingWindowStartTimeStep(
					PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getSamplingWindowStartTimeStep());
			acq.setTimeCompensationResolution(
					PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getTimeCompensationResolution());
			acq.setUpDownChirpTransmission(
					PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getUpDownChirpTransmission());
			acq.setWaveFormSelection(PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getWaveformSelection());
			acq.setDi2sPulseRepetitionInterval(
					PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getDi2sPulseRepetitionInterval());
			
			if (acq.getSubSwathNumber() != null)
				
				acq.setSubSwathNumber(PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getSubSwathNumber());
				// Add DTO subswaths
				for (Subswath subswath : PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId)
						.getSubswathParameter()) {					
					acq.addSubswathParameter(subswath);
				}
							
				if (acq.getDi2s() != null) {
					
					acq.setSubSwathNumber(2); // TODO: check ?

					/**
					 * The slave schedDTOId
					 */
					String slaveSchedDTOId = ObjectMapper.parseDMToSchedDTOId(acq.getDi2s().getUgsId(),
							acq.getDi2s().getProgrammingRequestId(), acq.getDi2s().getAcquisitionRequestId(),
							acq.getDi2s().getDtoId());
					
					// Add DTO slave subswaths
					if (PRListProcessor.dtoSchedIdMap.get(pSId).containsKey(slaveSchedDTOId)) {
						
						for (Subswath subswath : PRListProcessor.dtoSchedIdMap.get(pSId).get(slaveSchedDTOId)
								.getSubswathParameter()) {					
							acq.addSubswathParameter(subswath);
						}			
				}
			}
		}
	}

	/**
	 * Add info to the maneuver task according to SPARC output for equivalent
	 * Maneuver only
	 * // TODO: finalize
	 *
	 * @param man
	 * @param brmMan
	 * @param pSId
	 */
	public static void addEquivManInfo(Maneuver man, com.nais.spla.brm.library.main.ontology.tasks.Maneuver brmMan, 
			Long pSId) throws Exception {

		// Add lack Info for the Equivalent Maneuver	
			
		if (PRListProcessor.equivStartTimeManMap.get(pSId).containsKey(Long.toString(brmMan.getStartTime().getTime()))) {
		
			logger.debug("Add Maneuver Info for the equivalent DTO: " + brmMan.getReferredEquivalentDto());
			
			man.setSlewParameters(PRListProcessor.equivStartTimeManMap.get(pSId).get(Long.toString(brmMan.getStartTime().getTime())).getSlewParameters());
			man.setSplineBlock1(PRListProcessor.equivStartTimeManMap.get(pSId).get(Long.toString(brmMan.getStartTime().getTime())).getSplineBlock1());
			man.setTimeDloErosionList(PRListProcessor.equivStartTimeManMap.get(pSId).get(Long.toString(brmMan.getStartTime().getTime())).getTimeDloErosionList());			
			if (PRListProcessor.equivStartTimeManMap.get(pSId).get(Long.toString(brmMan.getStartTime().getTime())).hasPitchIntervals()) {
				man.addPitchIntervals(PRListProcessor.equivStartTimeManMap.get(pSId).get(Long.toString(brmMan.getStartTime().getTime())).retrievePitchIntervals());
			}
		}
	}
	
	/**
	 * Set BITE info to the acquisition task according to SPARC output
	 *
	 * @param task
	 * @param pSId
	 */
	public static void setBiteInfo(BITE bite, Long pSId) throws Exception {

		// Add lack Info for the BITE	

		/**
		 * The scheduled AR Id
		 */
		String schedARId = ObjectMapper.parseDMToSchedARId(bite.getUgsId(), bite.getProgrammingRequestId(),
				bite.getAcquisitionRequestId());
		
		/**
		 * The scheduled DTO Id
		 */
		String schedDTOId = ObjectMapper.parseDMToSchedDTOId(bite.getUgsId(), bite.getProgrammingRequestId(),
				bite.getAcquisitionRequestId(), bite.getDtoId());

		
		// TODO: check data wrt Acquisition data (+ EkOwner and page)
		
		if (bite.getEncryptionKeyMode() == null) {
			bite.setEncryptionKeyMode(true);
		}
		if (bite.getEncryptionKeyPos() == null) {
			bite.setEncryptionKeyPos(new Byte((byte) 0));
		}
		
		if (PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionEKSelection() != null) {
			bite.setEncryptionEKSelection(
					PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionEKSelection());
		}
		if (PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionEKIndex() != null) {
			bite.setEncryptionEKIndex1(PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionEKIndex());
		}
		if (PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionEKOwnerId() != null) {
			bite.setEncryptionEKOwnerId(
					PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionEKOwnerId());
		}
		if (PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionStrategy() != null) {
			bite.setEncryptionStrategy(
					PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionStrategy());
		} else {
			bite.setEncryptionStrategy("CLEAR");
		}
		if (PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionIVIndex() != null) {
			bite.setEncryptionIVIndex(PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionIVIndex());
		}
		if (PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionPageId() != null) {
			bite.setEncryptionPageId(PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionPageId());
		}
		if (PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionIVSelection() != null) {
			bite.setEncryptionIVSelection(
					PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionIVSelection());
		}
		
		if (PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getImageLength() != null) {
			bite.setImageLength(
					PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getImageLength());
		}
		
		bite.setCarrierL2Selection(false);
		bite.setAdditionalPar1(PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getAdditionalPar1());
		bite.setAdditionalPar2(PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getAdditionalPar2());
		bite.setAdditionalPar3(PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getAdditionalPar3());
		if (bite.getUgsId().startsWith("2")) {
			
			bite.setAdditionalPar3(Double.toString(bite.getPacketStoreSequenceId().doubleValue()));
		}
		bite.setAdditionalPar4(PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getAdditionalPar4());
		bite.setAdditionalPar5(PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getAdditionalPar5());	
	}

	/**
	 * Associate info to the store task according to SPARC output
	 *
	 * @param dwl
	 * @param pSId
	 * @throws Exception
	 */
	public static void setStoreInfo(Store dwl, Long pSId) throws Exception {

		// Add lack Info for the Download
			
		/**
		 * The scheduled AR Id
		 */
		String schedARId = ObjectMapper.parseDMToSchedARId(dwl.getUgsId(), dwl.getProgrammingRequestId(),
				dwl.getAcquisitionRequestId());

		/**
		 * The scheduled DTO Id
		 */
		String schedDTOId = ObjectMapper.parseDMToSchedDTOId(dwl.getUgsId(), dwl.getProgrammingRequestId(),
				dwl.getAcquisitionRequestId(), dwl.getDtoId());
		
		if (PRListProcessor.aRSchedIdMap.get(pSId).containsKey(schedARId)
				&& PRListProcessor.dtoSchedIdMap.get(pSId).containsKey(schedDTOId)) {
			
			if (PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getPacketNumberH() != null) {
				
				dwl.setSourcePacketNumberH((PRListProcessor.dtoSchedIdMap.get(pSId)
						.get(schedDTOId).getPacketNumberH().longValue()));
		   } else {
			   
			   dwl.setSourcePacketNumberH(0L);
		   }
			
			if (PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getPacketNumberV() != null) {
				
				dwl.setSourcePacketNumberV(PRListProcessor.dtoSchedIdMap.get(pSId)
						.get(schedDTOId).getPacketNumberV().longValue());
		   } else {
			   
			   dwl.setSourcePacketNumberV(0L);
		   }
		}
	}

	
	/**
	 * Associate info to the download task according to SPARC output
	 *
	 * @param pSId
	 * @param dwl
	 * @param polar
	 * @throws Exception
	 */
	public static void setDwlInfo(Long pSId, Download dwl, Polarization polar) throws Exception {

		/**
		 * Instance handlers
		 */
		EquivDTOHandler equivDTOHandler = new EquivDTOHandler();

			
		/**
		 * The scheduled AR Id
		 */
		String schedARId = ObjectMapper.parseDMToSchedARId(dwl.getUgsId(), dwl.getProgrammingRequestId(),
				dwl.getAcquisitionRequestId());

		/**
		 * The scheduled DTO Id
		 */
		String schedDTOId = ObjectMapper.parseDMToSchedDTOId(dwl.getUgsId(), dwl.getProgrammingRequestId(),
				dwl.getAcquisitionRequestId(), dwl.getDtoId());
		
		if (PRListProcessor.aRSchedIdMap.get(pSId).containsKey(schedARId)
				&& PRListProcessor.dtoSchedIdMap.get(pSId).containsKey(schedDTOId)) {

			if (polar.equals(Polarization.HH)
					&& PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getPacketNumberH() != null) {
				
				dwl.setSourcePacketNumberH(BigDecimal.valueOf(PRListProcessor.dtoSchedIdMap.get(pSId)
						.get(schedDTOId).getPacketNumberH()));
				dwl.setSourcePacketNumberV(BigDecimal.ZERO);
		   
			} else if (PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getPacketNumberV() != null) {
			   
			    dwl.setSourcePacketNumberV(BigDecimal.valueOf(PRListProcessor.dtoSchedIdMap.get(pSId)
						.get(schedDTOId).getPacketNumberV()));
			    dwl.setSourcePacketNumberH(BigDecimal.ZERO);
			}
			
			if (PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getImageLength() != null) {
				dwl.setImageLength(
						PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getImageLength());
			}
			
//			// set Dwl EnchryptionInfo
//			setDwlEnchryptionInfo(pSId, dwl, schedARId);
			
			// Updated on 29/08/2022 for the change of EnchryptionInfo
			if (RequestChecker.isDefence(ObjectMapper.getUgsId(schedARId))
					&& (equivDTOHandler.getDI2SVisibility(pSId, dwl) == 3)) {
			
				// Change download Encryption Info according to the scheduled DI2S Slave AR
				setDwlEnchryptionInfo(pSId, dwl, ObjectMapper.parseDMToSchedARId(dwl.getDi2s().getUgsId(), 
						dwl.getDi2s().getProgrammingRequestId(),
						dwl.getDi2s().getAcquisitionRequestId()));
			
			} else  {
				
				// Change download Encryption Info according to the scheduled AR
				setDwlEnchryptionInfo(pSId, dwl, schedARId);
			}
		}
	}

	/**
	 * TODO: finalize PassThrough task
	 *
	 * @param pt
	 * @param pSId
	 * @param sizeH
     * @param sizeV
	 * @throws Exception
	 */
	public static void setPTInfo(Long pSId, PassThrough pt, int sizeH, int sizeV) throws Exception {

		/**
		 * Instance handlers
		 */
		EquivDTOHandler equivDTOHandler = new EquivDTOHandler();
	
		/**
		 * The scheduled AR Id
		 */
		String schedARId = ObjectMapper.parseDMToSchedARId(pt.getUgsId(), pt.getProgrammingRequestId(),
				pt.getAcquisitionRequestId());

		/**
		 * The scheduled DTO Id
		 */
		String schedDTOId = ObjectMapper.parseDMToSchedDTOId(pt.getUgsId(), pt.getProgrammingRequestId(),
				pt.getAcquisitionRequestId(), pt.getDtoId());
		
		if (PRListProcessor.aRSchedIdMap.get(pSId).containsKey(schedARId)
				&& PRListProcessor.dtoSchedIdMap.get(pSId).containsKey(schedDTOId)) {

			if (PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getImageLength() != null) {
				pt.setImageLength(PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getImageLength());
			}
			
			if (sizeH > 0 && PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getPacketNumberH() != null) {
				pt.setSourcePacketNumberH(BigDecimal.valueOf(PRListProcessor.dtoSchedIdMap.get(pSId)
						.get(schedDTOId).getPacketNumberH()));
		    } else {			   
				pt.setSourcePacketNumberH(BigDecimal.ZERO);
		    }			
			if (sizeV > 0 && PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId).getPacketNumberV() != null) {				
				pt.setSourcePacketNumberV(BigDecimal.valueOf(PRListProcessor.dtoSchedIdMap.get(pSId)
						.get(schedDTOId).getPacketNumberV()));
		    } else {			   
				pt.setSourcePacketNumberV(BigDecimal.ZERO);
		    }
			
//			// Set PT Encryption Info
//			setPTEnchryptionInfo(pSId, pt, schedARId);
			
			// Updated on 29/08/2022 for the change of the EnchryptionInfo
			if (RequestChecker.isDefence(ObjectMapper.getUgsId(schedARId))
					&& equivDTOHandler.getDI2SVisibility(pSId, pt) == 3) {
			
				// Change download Encryption Info according to the scheduled DI2S Slave AR
				setPTEnchryptionInfo(pSId, pt, 
						ObjectMapper.parseDMToSchedARId(pt.getDi2s().getUgsId(), 
								pt.getDi2s().getProgrammingRequestId(),
								pt.getDi2s().getAcquisitionRequestId()));
			
			} else  {
				
				// Change download Encryption Info according to the scheduled AR
				setPTEnchryptionInfo(pSId, pt, schedARId);
			}
		}

	}
		
	/**	
	 * Set Download Enchryption Info 
	 * 
	 * @param pSId
	 * @param dwl
	 * @param schedARId
	 */
	public static void setDwlEnchryptionInfo(Long pSId, Download dwl, String schedARId) {
	
		// TODO: check data wrt Acquisition data (+ EkOwner and page)
		if (dwl.getEncryptionKeyMode() == null) {
			dwl.setEncryptionKeyMode(true);
		}		
		if (dwl.getEncryptionKeyPos() == null) {
			dwl.setEncryptionKeyPos(new Byte((byte) 0));
		}
		// dwl.setInizializationVectorMode(new Byte((byte) 0));
		// dwl.setInizializationVectorPos(new Byte((byte) 0));
		
		if (PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionEKSelection() != null) {
			dwl.setEncryptionEKSelection(
					PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionEKSelection());
		}
		if (PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionEKIndex() != null) {
			dwl.setEncryptionEKIndex1(PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId)
					.getEncryptionEKIndex());
		}
		if (PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionEKOwnerId() != null) {
			dwl.setEncryptionEKOwnerId(
					PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionEKOwnerId());
		}
		if (PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionStrategy() != null) {
			dwl.setEncryptionStrategy(
					PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionStrategy());
		} else {
			dwl.setEncryptionStrategy("CLEAR");
		}
		if (PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionIVIndex() != null) {
			dwl.setEncryptionIVIndex(PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionIVIndex());
		}
		if (PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionPageId() != null) {
			dwl.setEncryptionPageId(PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionPageId());
		}
		if (PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionIVSelection() != null) {
			dwl.setEncryptionIVSelection(
					PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionIVSelection());
		}					

	   dwl.setAdditionalPar1(PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getAdditionalPar1());
	   dwl.setAdditionalPar2(PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getAdditionalPar2());
	   dwl.setAdditionalPar3(PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getAdditionalPar3());
	   dwl.setAdditionalPar4(PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getAdditionalPar4());
	   dwl.setAdditionalPar5(PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getAdditionalPar5());
	   
	}
	
	/**	
	 * Set PassThrough Enchryption Info 
	 * 
	 * @param pSId
	 * @param pt
	 * @param schedARId
	 */
	public static void setPTEnchryptionInfo(Long pSId, PassThrough pt, String schedARId) {

		// TODO: check data wrt Acquisition data (+ EkOwner and page)
		if (pt.getEncryptionKeyMode() == null) {
			pt.setEncryptionKeyMode(true);
		}		
		if (pt.getEncryptionKeyPos() == null) {
			pt.setEncryptionKeyPos(new Byte((byte) 0));
		}
		// dwl.setInizializationVectorMode(new Byte((byte) 0));
		// dwl.setInizializationVectorPos(new Byte((byte) 0));
		
		if (PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionEKSelection() != null) {
			pt.setEncryptionEKSelection(
					PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionEKSelection());
		}
		if (PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionEKIndex() != null) {
			pt.setEncryptionEKIndex1(PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionEKIndex());
		}
		if (PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionEKOwnerId() != null) {
			pt.setEncryptionEKOwnerId(
					PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionEKOwnerId());
		}
		if (PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionStrategy() != null) {
			pt.setEncryptionStrategy(PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionStrategy());
		} else {
			pt.setEncryptionStrategy("CLEAR");
		}
		if (PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionIVIndex() != null) {
			pt.setEncryptionIVIndex(PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionIVIndex());
		}
		if (PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionPageId() != null) {
			pt.setEncryptionPageId(PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionPageId());
		}
		if (PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionIVSelection() != null) {
			pt.setEncryptionIVSelection(
					PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionIVSelection());
		}		

		pt.setAdditionalPar1(PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getAdditionalPar1());
		pt.setAdditionalPar2(PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getAdditionalPar2());
		pt.setAdditionalPar3(PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getAdditionalPar3());
		pt.setAdditionalPar4(PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getAdditionalPar4());
		pt.setAdditionalPar5(PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getAdditionalPar5());

	}
	
}
