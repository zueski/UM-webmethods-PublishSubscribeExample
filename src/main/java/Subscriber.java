/*
 *
 *   Copyright (c) 1999 - 2011 my-Channels Ltd
 *   Copyright (c) 2012 - 2016 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
 *
 *   Use, reproduction, transfer, publication or disclosure is prohibited except as specifically provided for in your License Agreement with Software AG.
 *
 */
package com.amway.integration.um.example;

import com.pcbsys.nirvana.client.nBaseClientException;
import com.pcbsys.nirvana.client.nChannel;
import com.pcbsys.nirvana.client.nChannelAlreadySubscribedException;
import com.pcbsys.nirvana.client.nChannelAttributes;
import com.pcbsys.nirvana.client.nChannelNotFoundException;
import com.pcbsys.nirvana.client.nConsumeEvent;
import com.pcbsys.nirvana.client.nEventListener;
import com.pcbsys.nirvana.client.nEventProperties;
import com.pcbsys.nirvana.client.nRequestTimedOutException;
import com.pcbsys.nirvana.client.nSecurityException;
import com.pcbsys.nirvana.client.nSelectorParserException;
import com.pcbsys.nirvana.client.nSelectorParserException;
import com.pcbsys.nirvana.client.nSessionFactory;
import com.pcbsys.nirvana.client.nSessionNotConnectedException;
import com.pcbsys.nirvana.client.nUnexpectedResponseException;
import com.pcbsys.nirvana.client.nUnknownRemoteRealmException;

import java.io.BufferedInputStream;
import java.util.Enumeration;
import java.util.Date;
import java.util.Properties;

/**
 * Subscribes to a nirvana channel
 */
public class Subscriber extends Client implements nEventListener 
{
	static long startEid;
	static String selector = null;
	
	private long lastEID = 0;
	private long startTime = 0;
	private long byteCount = 0;

	private int logLevel = 0;
	private int count = -1;
	private int totalMsgs = 0;
	private int reportCount = 10000;

	private nChannel myChannel;

	public Subscriber(Properties props)
	{
		super(props); 
		processEnvironmentVariables();
	}
	
	/**
	 * This method demonstrates the Nirvana API calls necessary to subscribe to
	 * a channel. It is called after all command line arguments have been
	 * received and validated
	 * 
	 * @param realmDetails
	 *            a String[] containing the possible RNAME values
	 * @param achannelName
	 *            the channel name to create
	 * @param selector
	 *            the subscription selector filter
	 * @param startEid
	 *            the eid to start subscribing from
	 * @param loglvl
	 *            the specified log level
	 * @param repCount
	 *            the specified report count
	 */
	private void doit(String[] realmDetails, String achannelName, String selector, long startEid, int loglvl, int repCount) 
	{
		logLevel = loglvl;
		reportCount = repCount;
		constructSession(realmDetails);
		// Subscribes to the specified channel
		try 
		{
			// Create a channel attributes object
			nChannelAttributes nca = new nChannelAttributes();
			nca.setName(achannelName);
			// Obtain the channel reference
			myChannel = mySession.findChannel(nca);
			// if the latest event has been implied (by specifying -1)
			if(startEid == -1) 
			{
				// Get the last eid on the channel and reset the start eid with
				// that value
				startEid = myChannel.getLastEID();
			}
			// Add this object as a subscribe to the channel with the specified
			// message selector channel and start eid
			myChannel.addSubscriber(this, selector, startEid);
			// Stay subscribed until the user presses any key
			System.out.println("Press any key to quit !");
			BufferedInputStream bis = new BufferedInputStream(System.in);
			try {
				bis.read();
			} catch (Exception read) {
			} // Ignore this
			System.out.println("Finished. Consumed total of " + totalMsgs);
			// Remove this subscriber
			myChannel.removeSubscriber(this);
		} catch (nChannelNotFoundException cnfe) {
			System.out.println("The channel specified could not be found.");
			System.out.println("Please ensure that the channel exists in the REALM you connect to.");
			cnfe.printStackTrace();
			System.exit(1);
		} catch (nSecurityException se) {
			System.out.println("Insufficient permissions for the requested operation.");
			System.out.println("Please check the ACL settings on the server.");
			se.printStackTrace();
			System.exit(1);
		} catch (nSessionNotConnectedException snce) {
			System.out.println("The session object used is not physically connected to the Nirvana Realm.");
			System.out.println("Please ensure the realm is running and check your RNAME value.");
			snce.printStackTrace();
			System.exit(1);
		} catch (nUnexpectedResponseException ure) {
			System.out.println("The Nirvana REALM has returned an unexpected response.");
			System.out.println("Please ensure the Nirvana REALM and client API used are compatible.");
			ure.printStackTrace();
			System.exit(1);
		} catch (nUnknownRemoteRealmException urre) {
			System.out.println("The channel specified resided in a remote realm which could not be found.");
			System.out.println("Please ensure the channel name specified is correct.");
			urre.printStackTrace();
			System.exit(1);
		} catch (nRequestTimedOutException rtoe) {
			System.out.println("The requested operation has timed out waiting for a response from the REALM.");
			System.out.println("If this is a very busy REALM ask your administrator to increase the client timeout values.");
			rtoe.printStackTrace();
			System.exit(1);
		} catch (nChannelAlreadySubscribedException chase) {
			System.out.println("You are already subscribed to this channel.");
			chase.printStackTrace();
			System.exit(1);
		} catch (nSelectorParserException spe) {
			System.out.println("An error occured while parsing the selector filter specified.");
			System.out.println("Please check the JMS documentation on how to write a valid selector.");
			spe.printStackTrace();
			System.exit(1);
		} catch (nBaseClientException nbce) {
			System.out.println("An error occured while creating the Channel Attributes object.");
			nbce.printStackTrace();
			System.exit(1);
		}
		// Close the session we opened
		try 
		{
			nSessionFactory.close(mySession);
		} catch (Exception ex) { }
		// Close any other sessions within this JVM so that we can exit
		nSessionFactory.shutdown();
	}
	
	/**
	 * A callback is received by the API to this method each time an event is
	 * received from the nirvana channel. Be carefull not to spend too much time
	 * processing the message inside this method, as until it exits the next
	 * message can not be pushed.
	 * 
	 * @param evt
	 *            An nConsumeEvent object containing the message received from
	 *            the channel
	 */
	public void go(nConsumeEvent evt) 
	{
		// If this is the first message we receive
		if(count == -1) 
		{	// Get a timestamp to be used for message rate calculations
			startTime = System.currentTimeMillis();
			count = 0;
		}
		// Increment he counter
		count++;
		totalMsgs++;
		// Have we reached the point where we need to report the rates?
		if(count == reportCount) 
		{
			// Reset the counter
			count = 0;
			// Get a timestampt to calculate the rates
			long end = System.currentTimeMillis();
			// Does the specified log level permits us to print on the screen?
			if(logLevel >= 1)
			{
				// Dump the rates on the screen
				if(end != startTime) 
				{
					System.out.println("Received " + reportCount + " in " + (end - startTime) + " Evt/Sec = " + ((reportCount * 1000) / (end - startTime)) + " Bytes/sec=" + ((byteCount * 1000) / (end - startTime)));
					System.out.println("Bandwidth data : Bytes Tx ["+ mySession.getOutputByteCount() + "] Bytes Rx [" + mySession.getInputByteCount() + "]");
				} else {
					System.out.println("Received " + reportCount + " faster than the system millisecond counter");
				}
			}
			// Set the startTime for the next report equal to the end timestamp
			// for the previous one
			startTime = end;
			// Reset the byte counter
			byteCount = 0;
		}
		// If the last EID counter is not equal to the current event ID
		if(lastEID != evt.getEventID()) 
		{
			// If yes, maybe we have missed an event, so print a message on the
			// screen.
			// This message could be printed for a number of other reasons.
			// One of them would be someone purging a range creating an 'eid
			// gap'.
			// As eids are never reused within a channel you could have a
			// situation
			// where this gets printed but nothing is missed.
			System.out.println("Expired event range " + (lastEID) + " - " + (evt.getEventID() - 1));
			// Reset the last eid counter
			lastEID = evt.getEventID() + 1;
		} else {
			// Increment the last eid counter
			lastEID++;
		}
		// Get the data of the message
		byte[] buffer = evt.getEventData();
		if(buffer != null) 
		{	// Add its length to the byte counter
			byteCount += buffer.length;
		}
		// If the loglevel permits printing on the screen
		if(logLevel >= 2) 
		{
			// Print the eid
			System.out.println("Event id : " + evt.getEventID());
			if(evt.isEndOfChannel()) 
			{	System.out.println("End of channel reached"); }
			// If the loglevel permits printing on the screen
			if(logLevel >= 3) 
			{
				// Print the message tag
				System.out.println("Event tag : " + evt.getEventTag());
				// Print the message data
				System.out.println("Event data : " + new String(evt.getEventData()));
				if(evt.hasAttributes()) 
				{	displayEventAttributes(evt.getAttributes()); }
				nEventProperties prop = evt.getProperties();
				if(prop != null) 
				{	displayEventProperties(prop); }
			}
		}
	}

	public static void main(String[] args) 
	{
		// Process command line arguments
		Properties props = processArgs(args);
		// Create an instance for this class
		Subscriber subscriber = new Subscriber(props);
		// Check the channel name specified
		String channelName = null;
		if(subscriber.getProperty("CHANNAME") != null)
		{
			channelName = subscriber.getProperty("CHANNAME");
		} else {
			Usage();
			System.exit(1);
		}
		startEid = -1; // Default value (points to last event in the channel +  1)
		// Check to see if a start EID value has been specified
		if(subscriber.getProperty("START") != null) 
		{
			try 
			{
				startEid = Integer.parseInt(subscriber.getProperty("START"));
			} catch (Exception num) { } // Ignore and use the defaults
		}
		int loglvl = 1; // Default value
		// Check to see if a LOGLEVEL value has been specified.
		if(subscriber.getProperty("DEBUG") != null) 
		{
			try
			{
				loglvl = Integer.parseInt(subscriber.getProperty("DEBUG"));
			} catch (Exception num) { } // Ignore and use the default
		}

		int reportCount = 10000; // Default value
		// Check to see if a value for report count has been specified. Every
		// time
		// N events get received where N = report count, a subscription rate
		// report will
		// be printed in System.out
		if(System.getProperty("COUNT") != null) 
		{
			try {
				reportCount = Integer.parseInt(System.getProperty("COUNT"));
			} catch (Exception num) { } // Ignore and use the default
		}

		// Check for a selector message filter value
		selector = System.getProperty("SELECTOR");

		// Check the local realm details
		int idx = 0;
		String RNAME = null;
		if(subscriber.getProperty("RNAME") != null)
		{
			RNAME = subscriber.getProperty("RNAME");
		} else {
			Usage();
			System.exit(1);
		}

		// Process the local REALM RNAME details
		String[] rproperties = new String[4];
		rproperties = parseRealmProperties(RNAME);

		// Subscribe to the channel specified
		subscriber.doit(rproperties, channelName, selector, startEid, loglvl, reportCount);
	}
	
	/**
	 * Prints the usage message for this class
	 */
	private static void Usage() 
	{
		System.out.println("Usage ...\n");
		System.out.println("  call with each setting as a key:value pair on commandline, e.g.:\n");
		System.out.println("    runSubscriber CHANNAME:sampleChannel SIZE:100 RNAME:nhp://uslxcd001619:9001 DEBUG:3\n");
		System.out.println("----------- Required Arguments> -----------\n");
		System.out.println("CHANNAME:  Channel name parameter for the channel to subscribe to");
		System.out.println("\n----------- Optional Arguments -----------\n");
		System.out.println("START:  The Event ID to start subscribing from");
		System.out.println("COUNT:  The number of events to wait before printing out summary information");
		System.out.println("SELECTOR:  The event filter string to use\n");
		System.out.println("DEBUG:  The level of output from each event, 0 - none, 1 - summary, 2 - EIDs, 3 - All (separate from api log level)");
		UsageEnv();
	}

} // End of subscriber Class

