package src.org.qcri.pipeline;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
  

public class Data {

 	public static void main(String[] args) throws IOException {
 	 
		Configuration config = new Configuration();
	 	initConfig(config);
		
		// Sets the maximum number of sources to be 10.
		int maxSrc = 10;
		for (int src = 1; src <= maxSrc; src += 1) {
			// Sets the source number in the config file.
			config.setNumSources(src);
			// Sets the min change duration in the experiment.
			for (int ch = 8; ch < 20; ch += 2) {
				long ms = System.currentTimeMillis();

				config.setMinChange(ch);
				// Sets the maximum hold duration in the experiment.
				config.setMaxHold(ch + 5);
				System.out.println("\nSources:" + config.getNumSources() + "\t"
						+ config.getMinChange() + "\t" + config.getMaxHold());

				cases(config);

				long ms2 = System.currentTimeMillis();
				System.out.print(" " + (ms2 - ms) / 1000.0);
			}
		} 
	}
	/**
	 * Creates the initial configuration, i.e., initial paramaters to be fed into the system.
	 * @param config a holder for the input parameters.
	 */
 
	private static void initConfig(Configuration config) {
		// set number of reference values. These are the left hand side values.
		config.setNumLhs(1000);
		// stable values are the default states for the experiments.
		//  We use these values to revert back to the default state
		// after an experiment before another experiment starts.
		config.setStableChange(200);
		config.setStableValueError(-1);
		config.setStableTimeError(-1);
		// Set the probability of reporting to be 100%.
		config.setStableProbReport(1000);
		
		
		config.setProbChange(config.getStableChange());
		config.setProbValueError(config.getStableValueError());
		config.setProbTimeError(config.getStableTimeError());
		config.setProbReport(config.getStableProbReport());
		config.setMaxHold(9);
	}

	/**
	 * Takes a configuration and runs experiments according to it. 
	 * @param config input parameters for the experiment.
	 * @throws IOException
	 */
	private static void cases(Configuration config) throws IOException {

		 
	// case 1
	// Runs the reporting probability experiment.
	config.updateConfig(1001, -1, -1, false, true, false, false);
	runCase(config);

	// case2
	// Runs the value error probability experiment.
	config.updateConfig(1000, -1, -1, false, false, false, true);
	runCase(config);

	// case3
	// Runs the time error probability experiment with probability of reporting set to 100%.
	config.updateConfig(1000, -1, -1, false, false, true, false);
	runCase(config);
	
	// case4
	// Runs the time error probability experiment with probability of reporting set to 40%.
	config.updateConfig(40, -1, -1, false, false, true, false);
	runCase(config);

	// case5
	// Runs the value error probability experiment with probability of reporting set to 40%.
		config.updateConfig(40, -1, -1, false, false, false, true);
	runCase(config);
	 
	// case 6
	int prReport = 900;
	config.setProbChange(200);
	int probValue = 100;
	int probTime = 100;
	config.updateConfig(prReport, probTime, probValue, true, false, false, false);
	runCase(config);
	
	}

	/**
	*  Takes a configuration and runs an experiment with that configuration.
	* 
	* @param config input params for the experiment.
	* @throws IOException
	*/
	private static void runCase(Configuration config) throws IOException {
		// Changes the probability of change in the experiment
		if (config.runChangeExp()) {
			for (int ch = 0; ch <= 1000; ch += 100) {
				config.setProbChange(ch);
				start(config);
			}
			config.setProbChange(config.getStableChange());
		}
		// Changes the probability of time error in the experiment.
		if (config.runValueExp()) {
			for (int val = 0; val < 800; val += 100) {
				config.setProbValueError(val);
				start(config);
			}
			config.setProbValueError(config.getStableValueError());
		}
		// Changes the probability of time error in the experiment.
		if (config.runTimeExp()) {
			for (int ti = 0; ti < 800; ti += 100) {
				config.setProbTimeError(ti);
				start(config);
			}
			config.setProbTimeError(config.getStableTimeError());
		}
		// Changes the probability of reporting in the experiment.
		if (config.runReportExp()) {
			for (int re = 1000; re >= 200; re -= 100) {
				config.setProbReport(re);
				start(config);
			}
			config.setProbReport(config.getStableProbReport());
		}
		// Used to run the last, composite experiment.
		if (config.runGeneralExp()) {
			start(config);
		}
	}
	/**
	 * Runs a configuration. 
	 */
	private static void start(Configuration config) throws IOException {

		Map<Integer, Long> errors = new HashMap<Integer, Long>();
		long probValueError = config.getProbValueError();
		// If it is not the last experiment.
		if(!config.generalExp){
			run(config,errors,probValueError);
		}
		else{
			// it is the last experiment.
			// ls holds the errors that we will add to each source.
			long [] ls = new long[] { 0, 100, 200 };
			for (long d : ls) {
				add(errors, d);
				run(config,errors,probValueError); 
			}
		}
		
	}
	
	private static void run(Configuration config, Map<Integer,Long> errors, long probValueError){
		// Holds the last time a reference value has changed its value.
		Map<Integer, Long> lastChangeTime = new HashMap<Integer, Long>();
		// Holds the last attribute value of a reference value.
		Map<Integer, Long> lastValue = new HashMap<Integer, Long>();
		int maxProb = 1000;
		// Sets the length of each stripe.
		// Creates a stripe 30 times the length of the minimum change duration.
		long stepCount = 30 * config.getMinChange();

		StringBuffer dataContent = new StringBuffer();
		
		// For each reference value
		for (int lhs = 0; lhs < config.getNumLhs(); lhs++) {
		
			for (long window = 1; window < stepCount; window++) {	
				// prob of change is independent of sources.
				// is it allowed to change?
				lastValue = changeAspect(config, lastChangeTime, lastValue,
						maxProb, lhs, window);
				// we have a specific value that is reported by a source
				// now.

				for (int src = 0; src < config.getNumSources(); src += 1) {

					// but will the source report it?
					if (actionIsOK(config.getProbReport(), maxProb)) {
						long val = lastValue.get(lhs);
						// And we are going to change it with errors.
						// should we add value error?
						long probValueError = config.getProbValueError();
						probValueError = errors.get(src);
						if (actionIsOK(probValueError, maxProb)) {
							// we add value error
							val = genRand(val);
						}
						long misstep = window;
						if (actionIsOK(config.getProbTimeError(), maxProb)) {
							// we add time error
							double ran = Math.random();
							int c = 1;
							if (ran > 0.5) {
								// negative
								c = -1;
							}
								misstep = window + c * new Random().nextInt(5);
						}
						dataContent.append(src + "\t" + lhs + "\t"
								+ misstep + "\t" + val + "\r\n");
					}
				}
				
			}
		}
		//This commented out code is the entry point for running the temporal experiments.
		// We are sending the file content that holds the synthetically generated input data.
		// SyntheticTimeConstraintEntrance.syn(dataContent);
	}

	private static void add(Map<Integer, Long> errors, long x) {
		errors.put(0, 100 + x);
		errors.put(1, 100 + x);
		errors.put(2, 300 + x);
		errors.put(3, 500 + x);
		errors.put(4, 100 + x);
		errors.put(5, 300 + x);
		errors.put(6, 500 + x);
		errors.put(7, 100 + x);
		errors.put(8, 300 + x);
		errors.put(9, 500 + x);
	}
	
	
	private static Map<Integer, Long> changeAspect(Configuration config,
			Map<Integer, Long> lastChangeTime, Map<Integer, Long> lastValue,
			int maxProb, int lhs, long step) {
		long val = -1;

		// case 1: No previous value
		if (!lastChangeTime.containsKey(lhs)) {
			val = genRand(-1);
			lastValue.put(lhs, val);
			lastChangeTime.put(lhs, step);
			return lastValue;
		}
		// case 2: It has to change because of the maxHold
		long l = lastChangeTime.get(lhs);
		if ((step - l) >= config.getMaxHold()) {
			val = genRand(lastValue.get(lhs));
			lastValue.put(lhs, val);
			lastChangeTime.put(lhs, step);
			return lastValue;
		}

		// case 3: It is still not past the min change. No change!
		if ((step - l) < config.getMinChange()) {
			return lastValue;
		}

		// case 4: It is allowed to change. But will it?
		if (actionIsOK(config.getProbChange(), maxProb)) {
			val = lastValue.get(lhs);
			val = genRand(val);
			lastValue.put(lhs, val);
			lastChangeTime.put(lhs, step);
		}

		return lastValue;
	}

	private static boolean actionIsOK(long prob, int maxProb) {
		long g = new Random().nextInt(maxProb);
		if (g <= prob) {
			return true;
		}
		return false;
	}

	/**
	 * Returns a random value.
	 * 
	 * @param val
	 *            value that is guaranteed to be different from the returned
	 *            value.
	 * @return random value between 0 to 1000.
	 */
	private static int genRand(long val) {
		int i;
		Random generator = new Random();
		i = generator.nextInt(1000);
		while (i == val) {
			i = genRand(val);
		}

		return i;
	}
 
}
