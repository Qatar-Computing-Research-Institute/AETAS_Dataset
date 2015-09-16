package src.org.qcri.pipeline;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import src.org.qcri.util.Cfg;
import src.org.qcri.util.FileUtils;

public class Data {

	private static final boolean firstFive = false;

	public static void main(String[] args) throws IOException {

		Cfg.projectGranIn = Cfg.MINUTE;

		Configuration config = new Configuration();
		Cfg.outputDir = Cfg.outputDir + "Stream/synthetic/";
		initConfig(config);
		if (firstFive) {
			initFile(Cfg.outputDir, "resFixed.txt");
			initFile(Cfg.outputDir, "resFixedCase1.txt");
			initFile(Cfg.outputDir, "resFixedCase2.txt");
			initFile(Cfg.outputDir, "resFixedCase3.txt");
			initFile(Cfg.outputDir, "resFixedCase5.txt");
			initFile(Cfg.outputDir, "resFixedCase4.txt");

			initFile(Cfg.outputDir, "resFixedCase4_400.txt");
			initFile(Cfg.outputDir, "resFixedCase4_800.txt");
			initFile(Cfg.outputDir, "resFixedCase4_1000.txt");
		}
		initFile(Cfg.outputDir, "resFixedCase6.txt");

		int maxSrc = 10;
		for (int src = 1; src <= maxSrc; src += 1) {
			config.setNumSources(src);
			for (int ch = 8; ch < 20; ch += 2) {
				long ms = System.currentTimeMillis();

				config.setMinChange(ch);
				config.setMaxHold(ch + 2);
				System.out.println("\n" + config.getNumSources() + "\t"
						+ config.getMinChange() + "\t" + config.getMaxHold());

				cases(config);

				long ms2 = System.currentTimeMillis();
				System.out.print(" " + (ms2 - ms) / 1000.0);
			}
		}
		FileUtils.closeFiles();
	}

	private static void initFile(String dir, String file) {
		FileUtils.resetFile(dir, file);
		String string = "sources\tinputChange\tinputHold\tprobChange\tprobValueError\tprobTimeError\tprobReportError"
				+ "\tMethod\tChange\tHold\tHoldMin\tHoldMax\r\n";
		FileUtils.store(dir + file, string);
	}

	private static void initConfig(Configuration config) {
		config.setNumLhs(1000);
		config.setStableChange(200);
		config.setProbChange(config.getStableChange());
		config.setStableValueError(-1);
		config.setProbValueError(config.getStableValueError());
		config.setStableTimeError(-1);
		config.setProbTimeError(config.getStableTimeError());
		config.setStableProbReport(1001);
		config.setProbReport(config.getStableProbReport());
		config.setMaxHold(9);
	}

	private static void cases(Configuration config) throws IOException {

		if (firstFive) {
			// case 1
			config.updateConfig(1001, -1, -1, false, true, false, false);
			Cfg.outputFile = "resFixedCase1.txt";

			runCase(config);

			// case2
			config.updateConfig(1001, -1, -1, false, false, false, true);
			Cfg.outputFile = "resFixedCase2.txt";
			runCase(config);

			// case3
			config.updateConfig(1001, -1, -1, false, false, true, false);
			Cfg.outputFile = "resFixedCase3.txt";
			runCase(config);
			// case4
			config.updateConfig(40, -1, -1, false, false, true, false);
			Cfg.outputFile = "resFixedCase4.txt";
			runCase(config);

			// case5
			config.updateConfig(40, -1, -1, false, false, false, true);
			Cfg.outputFile = "resFixedCase5.txt";
			runCase(config);
		}
		// case 6
		// for(
		int prReport = 900;
		// prReport>499;prReport-=100)
		{

			config.setProbChange(200);
			int probValue = 100;
			int probTime = 100;
			config.updateConfig(prReport, probTime, probValue, true, false,
					false, false);
			Cfg.outputFile = "resFixedCase6.txt";
			runCase(config);
		}
	}

	private static void runCase(Configuration config) throws IOException {

		if (config.runChangeExp()) {
			for (int ch = 0; ch <= 1001; ch += 100) {
				config.setProbChange(ch);
				start(config);
			}
			config.setProbChange(config.getStableChange());
		}
		if (config.runValueExp()) {
			for (int val = 0; val < 800; val += 100) {
				config.setProbValueError(val);
				start(config);
			}
			config.setProbValueError(config.getStableValueError());
		}
		if (config.runTimeExp()) {
			for (int ti = 0; ti < 800; ti += 100) {
				config.setProbTimeError(ti);
				start(config);
			}
			config.setProbTimeError(config.getStableTimeError());
		}
		if (config.runReportExp()) {
			for (int re = 1001; re >= 200; re -= 100) {
				config.setProbReport(re);
				start(config);
			}
			config.setProbReport(config.getStableProbReport());
		}
		if (config.runGeneralExp()) {
			start(config);
		}
	}

	private static void start(Configuration config) throws IOException {

		Map<Integer, Long> errors = new HashMap<Integer, Long>();
		for (long d : new long[] { 0, 100, 200 }) {
			add(errors, d);
			Cfg.prefix = getPrefix(config) + "\t" + (1 + d / 100);
			Map<Integer, Long> lastChangeTime = new HashMap<Integer, Long>();
			Map<Integer, Long> lastValue = new HashMap<Integer, Long>();
			int maxProb = 1001;
			Cfg.projectGranIn = (long) 1;
			long stepCount = 30 * config.getMinChange();

			StringBuffer dataContent = new StringBuffer();
			for (int lhs = 0; lhs < config.getNumLhs(); lhs++) {
				// prob of change is independent of sources.
				for (long step = 1; step < stepCount; step++) {
					// is it allowed to change?
					lastValue = changeAspect(config, lastChangeTime, lastValue,
							maxProb, lhs, step);

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
							long misstep = step;
							if (actionIsOK(config.getProbTimeError(), maxProb)) {
								// we add time error
								double ran = Math.random();
								int c = 1;
								if (ran > 0.5) {
									// negative
									c = -1;
								}

								misstep = step + c * new Random().nextInt(5);
							}
							dataContent.append(src + "\t" + lhs + "\t"
									+ misstep + "\t" + val + "\r\n");
						}
					}
					// System.out.println(err+" errors.");
				}

			}

			SyntheticTimeConstraintEntrance.syn(dataContent);
		}
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
		i = generator.nextInt(1001);
		while (i == val) {
			i = genRand(val);
		}

		return i;
	}

	private static String getPrefix(Configuration config) {
		return config.getNumSources() + "\t" + config.getMinChange() + "\t"
				+ config.getMaxHold() + "\t" + config.getProbChange() + "\t"
				+ config.getProbValueError() + "\t" + config.getProbTimeError()
				+ "\t" + (1000 - config.getProbReport());
	}

}
