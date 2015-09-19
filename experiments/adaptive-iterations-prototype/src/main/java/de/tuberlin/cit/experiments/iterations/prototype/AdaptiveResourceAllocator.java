package de.tuberlin.cit.experiments.iterations.prototype;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AdaptiveResourceAllocator {

	public static int computeOptimalParallelism(int parallelism, Map<String, Object> allAccumulatorResults) {

		List<List<Double>> cpuStatistics = filterOutCpuHistories(allAccumulatorResults);

		List<Double> cpuAverages = computeCpuAverages(cpuStatistics);

		Double accumulatedAverages = 0.0;
		for (Double avg : cpuAverages) {
			accumulatedAverages += avg;
		}
		Double averageAverage = accumulatedAverages / cpuAverages.size();

		int proposedParallelism = parallelism;


		// based on a few experiments with k-means it looks like (for k-means on wally) an average cpu utilization of
		// around 25% for each of the workers is best, so we'll scale up or down more or less aggressively to this
		// target utilization

		if (averageAverage > 0.50) {
			proposedParallelism = parallelism * 2;
		} else if (averageAverage > 0.30) {
			proposedParallelism = (int) Math.abs(parallelism * 1.25);
		} else if (averageAverage < 0.20) {
			proposedParallelism = (int) Math.abs(parallelism * 0.75);
		} else if (averageAverage < 0.05) {
			proposedParallelism = (int) Math.abs(parallelism * 0.5);
		}

		System.out.println("average CPU average: " + averageAverage);

		return proposedParallelism;
	}

	private static List<Double> computeCpuAverages(List<List<Double>> cpuStatistics) {

		List<Double> cpuAverages = new ArrayList<>();
		for (List<Double> cpuHistory : cpuStatistics) {
			int utilizationCount = 0;
			for (Double utilization : cpuHistory) {
				if (utilization > 0.5) {
					utilizationCount++;
				}
			}
			cpuAverages.add((double) utilizationCount / cpuHistory.size());
		}
		return cpuAverages;
	}

	private static List<List<Double>> filterOutCpuHistories(Map<String, Object> allAccumulatorResults) {

		List<List<Double>> cpuStatistics = new ArrayList<>();
		for (Map.Entry<String,Object> accumulatorResult : allAccumulatorResults.entrySet()) {
			if (accumulatorResult.getKey().contains("cpu utilization")) {
				cpuStatistics.add((List<Double>)accumulatorResult.getValue());
			}
		}
		return cpuStatistics;
	}

}
