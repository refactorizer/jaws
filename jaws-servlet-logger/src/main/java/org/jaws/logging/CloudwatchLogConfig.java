package org.jaws.logging;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class CloudwatchLogConfig {

	public CloudwatchLogConfig() {
		
		System.err.println(Instant.now() + ": " + getClass().getName() + " - class loaded");
		
		LogManager lm = LogManager.getLogManager();
		
		try(InputStream is = getClass().getClassLoader().getResourceAsStream("jaws-cloudwatch-logging.properties")) {
			lm.readConfiguration(is);
			Logger.getLogger("").getHandlers()[0].setFormatter(new SimpleFormatter());
			System.err.println(Instant.now() + " *** LOGGER INIT OK ***");
		} catch (SecurityException | IOException e) {
			System.err.println(Instant.now() + " *** FAILED TO INITIALIZE CLOUDWATCH LOGGING ***");
			e.printStackTrace();
		}
		
		System.err.println(Instant.now() + ": " + getClass().getName() + " - config completed");
	}
	
}
