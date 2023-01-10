/**
*
* MODULE FILE NAME: CSPS.java
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

package com.telespazio.csg.spla.csps.core.server;

import javax.jms.JMSException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nais.spla.msl.main.core.MSLConnectionElement;
import com.nais.spla.msl.main.core.MessageServiceLayer;
import com.telespazio.csg.spla.csps.core.message.impl.CSPSListener;
import com.telespazio.splaif.protobuf.ActivateMessage.Activate;
import com.telespazio.splaif.protobuf.ActivateMessage.ActivateAck;
import com.telespazio.splaif.protobuf.CloseSessionMessage.CloseSessionNotification;
import com.telespazio.splaif.protobuf.CloseSessionMessage.SetPlanningSessionStatus;
import com.telespazio.splaif.protobuf.ConflictReportMessage.AsyncConflictReport;
import com.telespazio.splaif.protobuf.ConflictReportMessage.ConflictReport;
import com.telespazio.splaif.protobuf.DI2SMessage.DI2SCompatibilityRequest;
import com.telespazio.splaif.protobuf.DI2SMessage.DI2SCompatibilityResult;
import com.telespazio.splaif.protobuf.EnforceExclusionRuleMessage.EnforceExclusionRuleRequest;
import com.telespazio.splaif.protobuf.EnforceExclusionRuleMessage.EnforceExclusionRuleResult;
import com.telespazio.splaif.protobuf.FilteringMessage.FilteringRequest;
import com.telespazio.splaif.protobuf.FilteringMessage.FilteringResult;
import com.telespazio.splaif.protobuf.NextARMessage.NextAR;
import com.telespazio.splaif.protobuf.NextARMessage.NextARSchedulabilityStatus;
import com.telespazio.splaif.protobuf.PRListMessage.ManualPRList;
import com.telespazio.splaif.protobuf.PRListMessage.PRList;
import com.telespazio.splaif.protobuf.SentinelInfoMessage;
import com.telespazio.splaif.protobuf.StatusCheckmessage.SCMCheck;
import com.telespazio.splaif.protobuf.StatusCheckmessage.SCMCheckResult;

import it.sistematica.spla.datamodel.core.exception.ConfigurationException;

/**
 *
 * The CSPS main class.
 *
 */
public class CSPS extends Thread {

	/**
	 * The proper logger
	 */
	private static Logger logger = LoggerFactory.getLogger(CSPS.class);

	/**
	 * The keep running boolean
	 */
	public volatile boolean keepRunning = true;

	/**
	 * The Message service layer
	 */
	private MessageServiceLayer msl;

	/**
	 * The MSL connection element
	 */
	private MSLConnectionElement element;

	/**
	 * The CSPS handler
	 *
	 * @param msl
	 */
	public CSPS(MessageServiceLayer msl) {
		this("CSPS", msl);
	}

	/**
	 * The CSPS handler
	 *
	 * @param name
	 * @param msl
	 */
	public CSPS(String name, MessageServiceLayer msl) {
		
		// Instance msl
		this.msl = msl;
		
		// Set name
		this.setName(name);
	}

	/**
	 * Run CSPS
	 */
	@Override
	public void run() {
		try {
			// Start Server
			startServer();
		} catch (Exception e) {
			logger.error("Error starting CSPS: " + e.getMessage(), e);
		}
	}

	/**
	 * Start the CSPS server
	 *
	 * @throws ConfigurationException
	 * @throws JMSException
	 * @throws Exception
	 */
	public void startServer() throws ConfigurationException, JMSException, Exception {
		try {

			// Initialize the CSPS configuration
			logger.info("***** Initialize CSPS configuration for CSPS-3.0");

			Configuration.initCSPS();

			// Setup the MSL
			logger.info("Setup the MSL.");
			setupMSL();

			logger.info(getName() + " started, waiting for messages...");

			while (this.keepRunning) {

				try {
					Thread.sleep(1000);

				} catch (InterruptedException ie) {

					logger.error("Error starting server: " + ie.getMessage());
				}
			}
		} catch (JMSException e) {
			logger.error("Error starting component : " + e.getMessage(), e);
		} finally {
			logger.info(getName() + " halted, cleanup resources.");
		}
	}

	/**
	 * Stop the CSPS Server
	 */
	public void stopServer() {
		this.keepRunning = false;
	}

	/**
	 * Setup MSL
	 * @throws JMSException
	 */
	protected void setupMSL() throws JMSException {
		
		// Create Subscriber
		this.element = this.msl.createSubscriber(Environment.getConfiguration().getActiveMQUser(),
				Environment.getConfiguration().getActiveMQPassword(), Environment.getConfiguration().getActiveMQURL(),
				Environment.getConfiguration().getModuleName());

		// Publish message classes to be received
		logger.info("Subscribing " + Activate.class.getSimpleName());
		this.msl.subscribeMessageToReceived(Environment.getConfiguration().getModuleName(),
				Activate.class.getSimpleName());

		logger.info("Subscribing " + PRList.class.getSimpleName());
		this.msl.subscribeMessageToReceived(Environment.getConfiguration().getModuleName(),
				PRList.class.getSimpleName());

		logger.info("Subscribing " + NextAR.class.getSimpleName());
		this.msl.subscribeMessageToReceived(Environment.getConfiguration().getModuleName(),
				NextAR.class.getSimpleName());

		logger.info("Subscribing " + CloseSessionNotification.class.getSimpleName());
		this.msl.subscribeMessageToReceived(Environment.getConfiguration().getModuleName(),
				CloseSessionNotification.class.getSimpleName());

		logger.info("Subscribing " + SetPlanningSessionStatus.class.getSimpleName());
		this.msl.subscribeMessageToReceived(Environment.getConfiguration().getModuleName(),
				SetPlanningSessionStatus.class.getSimpleName());

		logger.info("Subscribing " + DI2SCompatibilityResult.class.getSimpleName());
		this.msl.subscribeMessageToReceived(Environment.getConfiguration().getModuleName(),
				DI2SCompatibilityResult.class.getSimpleName());

		logger.info("Subscribing " + FilteringResult.class.getSimpleName());
		this.msl.subscribeMessageToReceived(Environment.getConfiguration().getModuleName(),
				FilteringResult.class.getSimpleName());

		logger.info("Subscribing " + EnforceExclusionRuleRequest.class.getSimpleName());
		this.msl.subscribeMessageToReceived(Environment.getConfiguration().getModuleName(),
				EnforceExclusionRuleRequest.class.getSimpleName());

		logger.info("Subscribing " + SCMCheckResult.class.getSimpleName());
		this.msl.subscribeMessageToReceived(Environment.getConfiguration().getModuleName(),
				SCMCheckResult.class.getSimpleName());

		logger.info("Subscribing " + ManualPRList.class.getSimpleName());
		this.msl.subscribeMessageToReceived(Environment.getConfiguration().getModuleName(),
				ManualPRList.class.getSimpleName());
		
		logger.info("Subscribing " + SentinelInfoMessage.class.getSimpleName());
		this.msl.subscribeMessageToReceived(Environment.getConfiguration().getModuleName(),
				SentinelInfoMessage.class.getSimpleName());

		logger.info("Publishing " + SCMCheck.class.getSimpleName());
		this.msl.subscribeMessageToSent(Environment.getConfiguration().getModuleName(), 
				SCMCheck.class.getSimpleName());

		// Publish message classes to be sent
		logger.info("Publishing " + ActivateAck.class.getSimpleName());
		this.msl.subscribeMessageToSent(Environment.getConfiguration().getModuleName(),
				ActivateAck.class.getSimpleName());

		logger.info("Publishing " + NextARSchedulabilityStatus.class.getSimpleName());
		this.msl.subscribeMessageToSent(Environment.getConfiguration().getModuleName(),
				NextARSchedulabilityStatus.class.getSimpleName());

		logger.info("Publishing " + ConflictReport.class.getSimpleName());
		this.msl.subscribeMessageToSent(Environment.getConfiguration().getModuleName(),
				ConflictReport.class.getSimpleName());

		logger.info("Publishing " + AsyncConflictReport.class.getSimpleName());
		this.msl.subscribeMessageToSent(Environment.getConfiguration().getModuleName(),
				AsyncConflictReport.class.getSimpleName());

		logger.info("Publishing " + DI2SCompatibilityRequest.class.getSimpleName());
		this.msl.subscribeMessageToSent(Environment.getConfiguration().getModuleName(),
				DI2SCompatibilityRequest.class.getSimpleName());

		logger.info("Publishing " + FilteringRequest.class.getSimpleName());
		this.msl.subscribeMessageToSent(Environment.getConfiguration().getModuleName(),
				FilteringRequest.class.getSimpleName());

		logger.info("Publishing " + EnforceExclusionRuleResult.class.getSimpleName());
		this.msl.subscribeMessageToSent(Environment.getConfiguration().getModuleName(),
				EnforceExclusionRuleResult.class.getSimpleName());
		
		logger.info("Publishing " + SentinelInfoMessage.class.getSimpleName());
		this.msl.subscribeMessageToSent(Environment.getConfiguration().getModuleName(),
				SentinelInfoMessage.class.getSimpleName());

		// Setup message listener
		logger.info("Registering CSPS Listener...");
		this.element.getConsumer().setMessageListener(new CSPSListener(this.element.getSession(), this.msl));

	}
}
