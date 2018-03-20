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
import com.pcbsys.nirvana.client.nChannelAttributes;
import com.pcbsys.nirvana.client.nChannelNotFoundException;
import com.pcbsys.nirvana.client.nConsumeEvent;
import com.pcbsys.nirvana.client.nProtobufEvent;
import com.pcbsys.nirvana.client.nEventProperties;
import com.pcbsys.nirvana.client.nRequestTimedOutException;
import com.pcbsys.nirvana.client.nSecurityException;
import com.pcbsys.nirvana.client.nSessionFactory;
import com.pcbsys.nirvana.client.nSessionNotConnectedException;
import com.pcbsys.nirvana.client.nUnexpectedResponseException;
import com.pcbsys.nirvana.client.nUnknownRemoteRealmException;

import java.net.InetAddress;
import java.util.TimeZone;
import java.util.Properties;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.UUID;

import com.amway.canonical.EventUDM;


/**
 * Publishes reliably to a nirvana channel
 */
public class Publisher extends Client
{
	private boolean isOk = true;
	private nBaseClientException asyncException;
	
	public Publisher(Properties props)
	{
		super(props); 
		processEnvironmentVariables();
	}
	
	/**
	 * This method demonstrates the Nirvana API calls necessary to publish to a
	 * channel. It is called after all command line arguments have been received
	 * and validated
	 * 
	 * @param realmDetails
	 *            a String[] containing the possible RNAME values
	 * @param achannelName
	 *            the channel name to publish to
	 * @param count
	 *            the number of messages to publish
	 */
	private void doit(String[] realmDetails, String achannelName, int count) 
	{
		constructSession(realmDetails);
		// Publishes to the specified channel
		try 
		{
			// Create a channel attributes object
			nChannelAttributes nca = new nChannelAttributes();
			nca.setName(achannelName);
			// Obtain a reference to the channel
			nChannel myChannel = mySession.findChannel(nca);
			// Inform the user that publishing is about to start
			System.out.println("Starting publish of " + count + " events");
			// Obtain the channel's last event ID prior to us publishing
			// anything
			long startEid = myChannel.getLastEID();
			// Get a timestamp to be used to calculate the message publishing
			// rates
			long start = System.currentTimeMillis();
			// Loop as many times as the number of messages we want to publish
			for (int x = 0; x < count; x++) 
			{
				//create builder for event, this should get encapsulated into its own class by schema probably
				String myEventID = "event_" + x;
				String myEventCode = "TestEventType";
				TimeZone tz = TimeZone.getTimeZone("UTC");
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'"); // Quoted "Z" to indicate UTC, no timezone offset
				df.setTimeZone(tz);
				String mySourceTimestamp = df.format(new Date());
				EventUDM.AmGlDocs_Event_EventUDM.Builder builder = EventUDM.AmGlDocs_Event_EventUDM.newBuilder();
				builder.setPayload("iter_"+x);
				// create nested document
				builder.getEventInfoBuilder()
					.setEventID(myEventID)
					.setEventCode(myEventCode)
					.setSourceTimestamp(mySourceTimestamp)
					.setSourceHost(InetAddress.getLocalHost().getHostName());
				// update envelope,
				String uuid = UUID.randomUUID().toString();
				builder.getEnvBuilder()
					.setLocale("")
					.setActivation(uuid)
					.setBusinessContext("message_id")
					.setUuid(uuid)
					.setPubId(uuid);
				// create event
				nConsumeEvent evt1 = new nProtobufEvent(builder.build().toByteArray(), "AmGlDocs_Event_EventUDM");
				//props.put("Doc", );
				//nConsumeEvent evt1 = new nConsumeEvent(props, builder.build().toByteArray());
				try 
				{
					// Publish the event
					myChannel.publish(evt1);
				} catch (nSessionNotConnectedException ex) {
					while(!mySession.isConnected()) 
					{
						System.out.println("Disconnected from Nirvana, Sleeping for 1 second...");
						try 
						{
							Thread.sleep(1000);
						} catch (InterruptedException e) { }
					}
					x--; 
					// We need to repeat the publish for the event publish
					// that caused the exception,
					// so we reduce the counter
				} catch (nBaseClientException ex) {
					System.out.println("Publish.java : Exception : " + ex.getMessage());
					throw ex;
				}
				// Check if an asynchronous exception has been received
				if(!isOk) 
				{
					// If it has, then throw it
					throw asyncException;
				}
			}
			// Calculate the actual number of events published by obtaining
			// the channel's last eid after our publishing and subtracting
			// the channel's last eid before our publishing.
			// This also ensures that all client queues have been flushed.
			long end = System.currentTimeMillis();
			System.out.println("Get Last EID");
			long events = myChannel.getLastEID() - startEid;
			System.out.println("Got Last EID");
			// Check if an asynchronous exception has been received
			if(!isOk) 
			{
				// If it has, then throw it
				throw asyncException;
			}
			// Get a timestamp to calculate the publishing rates
			// Calculate the events / sec rate
			long eventPerSec = (((events) * 1000) / ((end + 1) - start));
			// Calculate the bytes / sec rate
			// Inform the user of the resulting rates
			System.out.println("Publish Completed : ");
			System.out.println("[Events Published = " + events + "]  [Events/Second = " + eventPerSec + "]"); 
			System.out.println("Bandwidth data : Bytes Tx [" + mySession.getOutputByteCount() + "] Bytes Rx [" + mySession.getInputByteCount() + "]");
		} catch (nChannelNotFoundException cnfe) {
			System.out.println("The channel specified could not be found.");
			System.out.println("Please ensure that the channel exists in the Nirvana Realm you connect to.");
			cnfe.printStackTrace();
			System.exit(1);
		} catch (nSecurityException se) {
			System.out.println("Insufficient permissions for the requested operation.");
			System.out.println("Please check the ACL settings on the server.");
			se.printStackTrace();
			System.exit(1);
		} catch (nSessionNotConnectedException snce) {
			System.out.println("The session object used is not physically connected to the Nirvana Realm.");
			System.out.println("Please ensure the Nirvana Realm Server is up and check your RNAME value.");
			snce.printStackTrace();
			System.exit(1);
		} catch (nUnexpectedResponseException ure) {
			System.out.println("The Nirvana Realm Server has returned an unexpected response.");
			System.out.println("Please ensure the Nirvana Realm Server and client API used are compatible.");
			ure.printStackTrace();
			System.exit(1);
		} catch (nUnknownRemoteRealmException urre) {
			System.out.println("The channel specified resided in a remote Nirvana Realm which could not be found.");
			System.out.println("Please ensure the channel name specified is correct.");
			urre.printStackTrace();
			System.exit(1);
		} catch (nRequestTimedOutException rtoe) {
			System.out.println("The requested operation has timed out waiting for a response from the Realm,.");
			System.out.println("If this is a very busy Realm ask your administrator to increase the client timeout values.");
			rtoe.printStackTrace();
			System.exit(1);
		} catch (nBaseClientException nbce) {
			System.out.println("An error occured while creating the Channel Attributes object. (" + achannelName + ")");
			nbce.printStackTrace();
			System.exit(1);
		} catch (java.net.UnknownHostException uhe) {
			System.out.println("An error occured while getting the host name");
			uhe.printStackTrace();
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

	public static void main(String[] args) 
	{
		// Process command line arguments
		Properties props = processArgs(args);
		// Create an instance for this class
		Publisher publisher = new Publisher(props);
		// Check the channel name specified
		String channelName = null;
		if(publisher.getProperty("CHANNAME") != null)
		{
			channelName = publisher.getProperty("CHANNAME"); 
		} else {
			Usage();
			System.exit(1);
		}
		int count = 10; // default value
		// Check if the number of messages to be published has been specified
		if(publisher.getProperty("COUNT") != null) 
		{
			try 
			{
				count = Integer.parseInt(publisher.getProperty("COUNT"));
			} catch (Exception num) { } // Ignore and use the defaults
		}
		// Check the local realm details
		int idx = 0;
		String RNAME = null;
		if(publisher.getProperty("RNAME") != null)
		{
			RNAME = publisher.getProperty("RNAME");
		} else {
			Usage();
			System.exit(1);
		}
		// Process the local REALM RNAME details
		String[] rproperties = new String[4];
		rproperties = parseRealmProperties(RNAME);
		// Publish to the channel specified
		publisher.doit(rproperties, channelName, count);
	}

	/**
	 * Prints the usage message for this class
	 */
	private static void Usage() 
	{
		System.out.println("Usage ...\n");
		System.out.println("  call with each setting as a key:value pair on commandline, e.g.:\n");
		System.out.println("    runPublisher CHANNAME:sampleChannel SIZE:100 RNAME:nhp://uslxcd001619:9001\n");
		System.out.println("----------- Required Arguments> -----------\n");
		System.out.println("CHANNAME:  Channel name parameter for the channel to publish to");
		System.out.println("\n----------- Optional Arguments -----------\n");
		System.out.println("COUNT:  The number of events to publish (default: 10)");
		System.out.println("SIZE:  The size (bytes) of the event to publish (default: 100)");
		System.out.println("DEBUG:  The level of output from each event, 0 - none, 1 - summary, 2 - EIDs, 3 - All (separate from api log level)\n");
		UsageEnv();
	}

} // End of publish Class

