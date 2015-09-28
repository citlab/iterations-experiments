package de.tuberlin.cit.experiments.iterations.prototype;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AdaptiveResourceAllocator {

	public static int computeOptimalParallelism(Map<String, Object> allAccumulatorResults) {

		Integer currentParallelism = retrieveJobDop(allAccumulatorResults);
		Integer maxParallelism = retrieveTotalNumberOfSlots(allAccumulatorResults);
		List<List<Double>> cpuStatistics = retrieveValidCpuHistories(allAccumulatorResults);

		System.out.println("AI - cpuStatistics: " + cpuStatistics);

		if (cpuStatistics.isEmpty()) {
			return currentParallelism;
		}

		List<Double> cpuAverages = computeCpuAverages(cpuStatistics);

		Double accumulatedAverages = 0.0;
		for (Double avg : cpuAverages) {
			accumulatedAverages += avg;
		}
		Double averageAverage = accumulatedAverages / cpuAverages.size();

		System.out.println("AI - average CPU average: " + averageAverage);

		int newParallelism = currentParallelism;

		// based on a few experiments with k-means it looks like (for k-means on wally) an average cpu utilization of
		// around 25% for each of the workers is best, so we'll scale up or down more or less aggressively to this
		// target utilization

		if (averageAverage > 0.50) {
			newParallelism = currentParallelism * 2;
		} else if (averageAverage > 0.30) {
			newParallelism = (int) Math.abs(currentParallelism * 1.25);
		} else if (averageAverage < 0.20) {
			newParallelism = (int) Math.abs(currentParallelism * 0.75);
		} else if (averageAverage < 0.05) {
			newParallelism = (int) Math.abs(currentParallelism * 0.5);
		}

		if (newParallelism < 1) {
			newParallelism = 1;
		} else if (newParallelism > maxParallelism) {
			newParallelism = maxParallelism;
		}

		return newParallelism;
	}

	private static List<Double> computeCpuAverages(List<List<Double>> cpuStatistics) {
		List<Double> cpuAverages = new ArrayList<>();
		for (List<Double> cpuHistory : cpuStatistics) {
			Double aggregatedUtilization = 0.0;
			for (Double utilization : cpuHistory) {
				aggregatedUtilization += utilization;
			}
			cpuAverages.add(aggregatedUtilization / cpuHistory.size());
		}
		return cpuAverages;
	}

	public static Integer retrieveJobDop(Map<String, Object> allAccumulatorResults) {
		for (Map.Entry<String,Object> accumulatorResult : allAccumulatorResults.entrySet()) {
			if (accumulatorResult.getKey().contains("jobDop")) {
				return (Integer)accumulatorResult.getValue();
			}
		}
		return null;
	}

	public static Integer retrieveTotalNumberOfSlots(Map<String, Object> allAccumulatorResults) {
		for (Map.Entry<String,Object> accumulatorResult : allAccumulatorResults.entrySet()) {
			if (accumulatorResult.getKey().contains("totalNumberOfSlots")) {
				return (Integer)accumulatorResult.getValue();
			}
		}
		return null;
	}

	private static List<List<Double>> retrieveValidCpuHistories(Map<String, Object> allAccumulatorResults) {
		List<List<Double>> cpuStatistics = new ArrayList<>();
		for (Map.Entry<String,Object> accumulatorResult : allAccumulatorResults.entrySet()) {
			if (accumulatorResult.getKey().contains("cpu utilization")) {

				List<Double> cpuHistory = (List<Double>)accumulatorResult.getValue();
				if (!cpuHistory.isEmpty()) {
					cpuStatistics.add(cpuHistory);
				}
			}
		}
		return cpuStatistics;
	}

}
