package gov.usgs.cida.nar.util;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class Profiler {

	public static final Logger log = LoggerFactory.getLogger("download-profiler");
	
	private static Map<UUID, Long> startTimes = new HashMap<>();
	
	public static UUID startTimer() {
		UUID uuid = UUID.randomUUID();
		long startTime = System.currentTimeMillis();
		startTimes.put(uuid, startTime);
		return uuid;
	}
	
	public static long stopTimer(UUID uuid) {
		long totalTime = -1;
		long endTime = System.currentTimeMillis();
		Long startTime = startTimes.remove(uuid);
		if (startTime != null) {
			totalTime = endTime - startTime;
		}
		return totalTime;
	}
}
