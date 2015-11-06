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


package de.tuberlin.cit.experiments.iterations.flink.multijobiterations;

import de.tuberlin.cit.experiments.iterations.flink.shared.AbstractPageRank;
import de.tuberlin.cit.experiments.iterations.flink.util.AccumulatorUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TypeSerializerInputFormat;
import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;

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
public class PageRankDelta extends AbstractPageRank {

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
		DataSet<Tuple2<Long, Double>> delta = pagesWithRanks.map(new InitialDeltaBuilder(numPages));

		// build adjacency list from link input
		DataSet<Tuple2<Long, Long[]>> adjacencyListInput =
				links.groupBy(0).reduceGroup(new BuildOutgoingEdgeList());

		long activePages = delta.count();

		// Run Page Rank for maxIterations
		for(int i = 0; i < maxIterations && activePages > 0; i++){

			// read intermediate results
			if(i > 0){
				delta = env.readFile(new TypeSerializerInputFormat<>((delta.getType())),
						(intermediateResultsPath + "/iteration_delta_" + Integer.toString(i - 1)));
				pagesWithRanks = env.readFile(new TypeSerializerInputFormat<>((pagesWithRanks.getType())),
						(intermediateResultsPath + "/iteration_solution_" + Integer.toString(i - 1)));
			}

			delta = delta.join(adjacencyListInput).where(0).equalTo(0).with(new DeltaDistributorAndStat(DAMPENING_FACTOR))
					.groupBy(0)
					.reduceGroup(new AggAndFilterStat(threshold));
			activePages = delta.count();
			pagesWithRanks = pagesWithRanks.join(delta).where(0).equalTo(0).with(new SolutionJoinAndStat());

			// Write intermediate results for next iteration
			if(i < maxIterations && activePages > 0) {
				delta.write(new TypeSerializerOutputFormat<Tuple2<Long, Double>>(),
						(intermediateResultsPath + "/iteration_delta_" + Integer.toString(i)), FileSystem.WriteMode.OVERWRITE);
				pagesWithRanks.write(new TypeSerializerOutputFormat<Tuple2<Long, Double>>(),
						(intermediateResultsPath + "/iteration_solution_" + Integer.toString(i)), FileSystem.WriteMode.OVERWRITE);

				JobExecutionResult jobResult = env.execute("Connected Components multi job");
				System.out.println("Active pages after iteration: " + activePages);
				AccumulatorUtils.dumpAccumulators(jobResult, i + 1);

			// emit final result
			} else {
				pagesWithRanks.writeAsCsv(outputPath, "\n", " ", FileSystem.WriteMode.OVERWRITE);

				JobExecutionResult result = env.execute("Page Rank multi job with deltas");
				AccumulatorUtils.dumpAccumulators(result, i);
			}
		}
	}
	
	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************


	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************
	
	private static String linksPath = null;
	private static String outputPath = null;
	private static long numPages = 0;
	private static int maxIterations = 10;
	private static String intermediateResultsPath = null;
	private static double threshold = 0.1;
	
	private static boolean parseParameters(String[] programArguments) {
		if(programArguments.length >= 5) {
			linksPath = programArguments[0];
			outputPath = programArguments[1];
			numPages = Long.parseLong(programArguments[2]);
			maxIterations = Integer.parseInt(programArguments[3]);
			intermediateResultsPath = programArguments[4];

			if (programArguments.length == 6) {
				threshold = Double.parseDouble(programArguments[5]);
			}
		} else {
			System.err.println("Usage: PageRank <links path> " +
					"<result path> <num pages> <max number of iterations> <intermediate-result-path> [threshold]");
			return false;
		}

		return true;
	}

	@Override
	public String getDescription() {
		return "Parameters: <links-path> <result-path> <num-pages> <max-number-of-iterations> <intermediate-result-path> [threshold]";
	}

}
