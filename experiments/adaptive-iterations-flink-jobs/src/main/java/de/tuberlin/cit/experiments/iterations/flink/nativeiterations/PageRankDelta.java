/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package de.tuberlin.cit.experiments.iterations.flink.nativeiterations;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * An implementation of the page rank algorithm, using native iteration and deltas.
 *
 * Based on http://data-artisans.com/data-analysis-with-flink-a-case-study-and-tutorial/
 *
 * <p>
 * Input files are plain text files and must be formatted as follows:
 * <ul>
 * <li>Edges are represented as pairs for vertex IDs which are separated by tab
 * characters. Edges are separated by new-line characters.<br>
 * For example <code>"1 2\n2 12\n1 12\n42 63"</code> gives four (undirected) edges (1)-(2), (2)-(12), (1)-(12), and (42)-(63).
 * </ul>
 * 
 */
@SuppressWarnings("serial")
public class PageRankDelta implements ProgramDescription {

	private static final double DAMPENING_FACTOR = 0.85;

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************
	
	public static void main(String... args) throws Exception {
		
		if(!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Long, Long>> links = env.readTextFile(linksPath).filter(new FilterComment()).flatMap(new UndirectEdge());

		// assign initial rank to pages
		DataSet<Tuple2<Long, Double>> pagesWithRanks = links.groupBy(0).reduceGroup(new RankAssigner(1.0d)); // 1.0d / numPages ?
		DataSet<Tuple2<Long, Double>> initialDeltas = pagesWithRanks.map(new InitialDeltaBuilder(numPages));

		// build adjacency list from link input
		DataSet<Tuple2<Long, Long[]>> adjacencyListInput =
				links.groupBy(0).reduceGroup(new BuildOutgoingEdgeList());

		DeltaIteration<Tuple2<Long, Double>, Tuple2<Long, Double>> adaptiveIteration = pagesWithRanks.iterateDelta(initialDeltas, maxIterations, 0);

		DataSet<Tuple2<Long, Double>> deltas = adaptiveIteration.getWorkset()
				.join(adjacencyListInput).where(0).equalTo(0).with(new DeltaDistributorAndStat(DAMPENING_FACTOR))
				.groupBy(0)
				.reduceGroup(new AggAndFilterStat(threshold));

		DataSet<Tuple2<Long, Double>> rankUpdates = adaptiveIteration.getSolutionSet()
				.join(deltas).where(0).equalTo(0).with(new SolutionJoinAndStat());

		DataSet<Tuple2<Long, Double>> finalPageRanks = adaptiveIteration.closeWith(rankUpdates, deltas);

		// Statistics
//		long count = pagesWithRanks.count();
//		System.err.println("Found " + count + " pages in dataset.");

		// emit result
		finalPageRanks.writeAsCsv(outputPath, "\n", " ", FileSystem.WriteMode.OVERWRITE);

		// execute program
		try {
			JobExecutionResult result = env.execute("Page Rank Iteration with Deltas");
			Map<String, Object> accumulators = result.getAllAccumulatorResults();
			List<String> keys = new ArrayList<String>(accumulators.keySet());
			Collections.sort(keys);
			System.out.println("Accumulators:");
			for (String key : keys) {
				System.out.println(key + " : " + accumulators.get(key));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/**
	 * A map function that assigns an initial rank to all pages.
	 */
	public static final class RankAssigner implements GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Double>>  {
		private final double rank;

		public RankAssigner(double rank) {
			this.rank = rank;
		}

		@Override
		public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Double>> out) throws Exception {
			long id = values.iterator().next().f0;
			out.collect(new Tuple2<Long, Double>(id, rank));
		}
	}

	/**
	 * A reduce function that takes a sequence of edges and builds the adjacency list for the vertex where the edges
	 * originate. Run as a pre-processing step.
	 */
	@ForwardedFields("0")
	public static final class BuildOutgoingEdgeList implements GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long[]>> {

		private final ArrayList<Long> neighbors = new ArrayList<Long>();

		@Override
		public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long[]>> out) {
			neighbors.clear();
			Long id = 0L;

			for (Tuple2<Long, Long> n : values) {
				id = n.f0;
				neighbors.add(n.f1);
			}

			out.collect(new Tuple2<Long, Long[]>(id, neighbors.toArray(new Long[neighbors.size()])));
		}
	}

	@ForwardedFields("0")
	public static final class InitialDeltaBuilder implements MapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {

		private final double uniformRank;

		public InitialDeltaBuilder(long numVertices) {
			this.uniformRank = 1.0 / numVertices;
		}

		@Override
		public Tuple2<Long, Double> map(Tuple2<Long, Double> initialRank) {
			initialRank.f1 -= uniformRank;
			return initialRank;
		}
	}

	public static final class DeltaDistributorAndStat extends RichFlatJoinFunction<Tuple2<Long, Double>, Tuple2<Long, Long[]>, Tuple2<Long, Double>> {

		private final Tuple2<Long, Double> tuple = new Tuple2<Long, Double>();

		private final double dampeningFactor;

		private long inCount;
		private long outCount;

		public DeltaDistributorAndStat(double dampeningFactor) {
			this.dampeningFactor = dampeningFactor;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			inCount = 0;
			outCount = 0;
		}

		@Override
		public void join(Tuple2<Long, Double> deltaFromPage, Tuple2<Long, Long[]> neighbors, Collector<Tuple2<Long, Double>> out) {
			Long[] targets = neighbors.f1;

			double deltaPerTarget = dampeningFactor * deltaFromPage.f1 / targets.length;

			tuple.f1 = deltaPerTarget;
			for (long target : targets) {
				tuple.f0 = target;
				out.collect(tuple);
			}

			inCount++;
			outCount += targets.length;
		}

		@Override
		public void close() throws Exception {
			int subtask = getIterationRuntimeContext().getIndexOfThisSubtask();
			int iteration = getIterationRuntimeContext().getSuperstepNumber();
			getRuntimeContext().getLongCounter("joinDeltaDistributor-iter_"+iteration+"-subtask_" + subtask + "-in").add(inCount);
			getRuntimeContext().getLongCounter("joinDeltaDistributor-iter_"+iteration+"-subtask_" + subtask + "-out").add(outCount);
		}
	}

	@ForwardedFields("0")
	public static final class AggAndFilter implements GroupReduceFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {

		private final double threshold;

		public AggAndFilter(double threshold) {
			this.threshold = threshold;
		}

		@Override
		public void reduce(Iterable<Tuple2<Long, Double>> values, Collector<Tuple2<Long, Double>> out)  {
			Long key = null;
			double delta = 0.0;

			for (Tuple2<Long, Double> t : values) {
				key = t.f0;
				delta += t.f1;
			}

			if (Math.abs(delta) > threshold) {
				out.collect(new Tuple2<Long, Double>(key, delta));
			}
		}
	}

	@ForwardedFields("0")
	public static final class AggAndFilterStat extends RichGroupReduceFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {

		private final double threshold;
		private long inCount;
		private long outCount;

		public AggAndFilterStat(double threshold) {
			this.threshold = threshold;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			inCount = 0;
			outCount = 0;
		}

		@Override
		public void reduce(Iterable<Tuple2<Long, Double>> values, Collector<Tuple2<Long, Double>> out)  {
			Long key = null;
			double delta = 0.0;

			for (Tuple2<Long, Double> t : values) {
				key = t.f0;
				delta += t.f1;
				inCount++;
			}

			if (Math.abs(delta) > threshold) {
				out.collect(new Tuple2<Long, Double>(key, delta));
				outCount++;
			}
		}

		@Override
		public void close() throws Exception {
			int subtask = getIterationRuntimeContext().getIndexOfThisSubtask();
			int iteration = getIterationRuntimeContext().getSuperstepNumber();
			getRuntimeContext().getLongCounter("reduceAggAndFilter-iter_"+iteration+"-subtask_" + subtask + "-in").add(inCount);
			getRuntimeContext().getLongCounter("reduceAggAndFilter-iter_"+iteration+"-subtask_" + subtask + "-out").add(outCount);
		}
	}

	@FunctionAnnotation.ForwardedFieldsFirst("0")
	@FunctionAnnotation.ForwardedFieldsSecond("0")
	public static final class SolutionJoinAndStat extends RichJoinFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, Tuple2<Long, Double>> {
		private long inCount;
		private long outCount;

		@Override
		public void open(Configuration parameters) throws Exception {
			inCount = 0;
			outCount = 0;
		}

		@Override
		public Tuple2<Long, Double> join(Tuple2<Long, Double> page, Tuple2<Long, Double> delta) {
			page.f1 += delta.f1;
			inCount++;
			outCount++;
			return page;
		}

		@Override
		public void close() throws Exception {
			int subtask = getIterationRuntimeContext().getIndexOfThisSubtask();
			int iteration = getIterationRuntimeContext().getSuperstepNumber();
			getRuntimeContext().getLongCounter("joinSolution-iter_"+iteration+"-subtask_" + subtask + "-in").add(inCount);
			getRuntimeContext().getLongCounter("joinSolution-iter_"+iteration+"-subtask_" + subtask + "-out").add(outCount);
		}
	}
	
	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************
	
	private static String linksPath = null;
	private static String outputPath = null;
	private static long numPages = 0;
	private static int maxIterations = 10;
	private static double threshold = 0;
	
	private static boolean parseParameters(String[] programArguments) {

		// parse input arguments
		if(programArguments.length == 4) {
			linksPath = programArguments[0];
			outputPath = programArguments[1];
			numPages = Long.parseLong(programArguments[2]);
			maxIterations = Integer.parseInt(programArguments[3]);
			threshold = 0.0001 / numPages;
		} else {
			System.err.println("Usage: PageRank <links path> " +
					"<result path> <num pages> <max number of iterations>");
			return false;
		}

		return true;
	}

	@Override
	public String getDescription() {
		return "Parameters: <links-path> <result-path> <num-pages> <max-number-of-iterations>";
	}

	/**
	 * Function that filter out the comment lines.
	 */
	public static final class FilterComment implements FilterFunction<String> {

		@Override
		public boolean filter(String s) throws Exception {
			return !s.startsWith("%");
		}
	}

	/**
	 * Undirected edges by emitting for each input edge the input edges itself and an inverted
	 * version.
	 */
	public static final class UndirectEdge implements FlatMapFunction<String, Tuple2<Long, Long>> {

		@Override
		public void flatMap(String edge, Collector<Tuple2<Long, Long>> out) {
			String[] line = edge.split("\t");
			Long v1 = Long.parseLong(line[0]);
			Long v2 = Long.parseLong(line[1]);
			out.collect(new Tuple2<Long, Long>(v1, v2));
			out.collect(new Tuple2<Long, Long>(v2, v1));
		}
	}
}
