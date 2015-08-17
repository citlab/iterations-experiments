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

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.io.TypeSerializerInputFormat;
import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.record.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.examples.java.graph.util.ConnectedComponentsData;

import java.util.Iterator;

/**
 * An implementation of the connected components algorithm, using a delta iteration.
 *
 * <p>
 * Initially, the algorithm assigns each vertex an unique ID. In each step, a vertex picks the minimum of its own ID and its
 * neighbors' IDs, as its new ID and tells its neighbors about its new ID. After the algorithm has completed, all vertices in the
 * same component will have the same ID.
 *
 * <p>
 * A vertex whose component ID did not change needs not propagate its information in the next step. Because of that,
 * the algorithm is easily expressible via a delta iteration. We here model the solution set as the vertices with
 * their current component ids, and the workset as the changed vertices. Because we see all vertices initially as
 * changed, the initial workset and the initial solution set are identical. Also, the delta to the solution set
 * is consequently also the next workset.<br>
 *
 * <p>
 * Input files are plain text files and must be formatted as follows:
 * <ul>
 * <li>Vertices represented as IDs and separated by new-line characters.<br> 
 * For example <code>"1\n2\n12\n42\n63"</code> gives five vertices (1), (2), (12), (42), and (63).
 * <li>Edges are represented as pairs for vertex IDs which are separated by space 
 * characters. Edges are separated by new-line characters.<br>
 * For example <code>"1 2\n2 12\n1 12\n42 63"</code> gives four (undirected) edges (1)-(2), (2)-(12), (1)-(12), and (42)-(63).
 * </ul>
 *
 * <p>
 * Usage: <code>ConnectedComponents &lt;vertices path&gt; &lt;edges path&gt; &lt;result path&gt; &lt;max number of iterations&gt;</code><br>
 * If no parameters are provided, the program is run with default data from {@link ConnectedComponentsData} and 10 iterations. 
 *
 * <p>
 * This example shows how to use:
 * <ul>
 * <li>Delta Iterations
 * <li>Generic-typed Functions 
 * </ul>
 */
@SuppressWarnings("serial")
public class ConnectedComponents implements ProgramDescription {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String... args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Integer, Integer>> edges = env.readTextFile(edgesPath).filter(new FilterComment()).flatMap(new UndirectEdge());

		// TODO: bug to be fixed due to iteration head deadlock:
		// https://issues.apache.org/jira/browse/FLINK-1088.
		DataSet<Tuple2<Integer, Integer>> edges2 = env.readTextFile(edgesPath).filter(new FilterComment()).flatMap(new UndirectEdge());

		// assign the initial components (equal to the vertex id)
		DataSet<Tuple2<Integer, Integer>> verticesWithInitialId = edges2.groupBy(0).reduceGroup(new InitialValue());
		DataSet<Tuple2<Integer, Integer>> delta = verticesWithInitialId;
		DataSet<Tuple2<Integer, Integer>> newSolutionSet = verticesWithInitialId;

		//Run Connected Components for maxIterations
		for(int i = 0; i < maxIterations; i++){
			//read in files
			if(i > 0){
				//Read in parameters
				delta = env.readFile(new TypeSerializerInputFormat<Tuple2<Integer, Integer>>((delta.getType())),
						(intermediateResultsPath + "/iteration_delta_" + Integer.toString(i - 1)));
				newSolutionSet = env.readFile(new TypeSerializerInputFormat<Tuple2<Integer, Integer>>((newSolutionSet.getType())),
						(intermediateResultsPath + "/iteration_solution_" + Integer.toString(i - 1)));
			}

			// apply the step logic: join with the edges, select the minimum neighbor, update if the
			// component of the candidate is smaller
			delta = delta.join(edges, JoinOperatorBase.JoinHint.REPARTITION_HASH_FIRST)
					.where(0)
					.equalTo(0)
					.with(new NeighborWithComponentIDJoin())
					.groupBy(0)
					.aggregate(Aggregations.MIN, 1)
					.join(newSolutionSet)
					.where(0)
					.equalTo(0)
					.with(new ComponentIdFilter());

			newSolutionSet = newSolutionSet.coGroup(delta).where(0).equalTo(0).with(new UpdateSolutionSet());

			//Write out for next iteration
			if(i != maxIterations -1) {
				delta.write(new TypeSerializerOutputFormat<Tuple2<Integer, Integer>>(),
						(intermediateResultsPath + "/iteration_delta_" + Integer.toString(i)), FileSystem.WriteMode.OVERWRITE);
				newSolutionSet.write(new TypeSerializerOutputFormat<Tuple2<Integer, Integer>>(),
						(intermediateResultsPath + "/iteration_solution_" + Integer.toString(i)), FileSystem.WriteMode.OVERWRITE);
				env.execute("Connected Components Bulk Iteration");
			}
		}

		// emit result
		newSolutionSet.writeAsCsv(outputPath, "\n", " ", FileSystem.WriteMode.OVERWRITE);

		// execute program
		try {
			env.execute("Connected Components Bulk Iteration");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/**
	 * Function that turns a value into a 2-tuple where both fields are that value.
	 */
	@ForwardedFields("*->f0")
	public static final class DuplicateValue<T> implements MapFunction<T, Tuple2<T, T>> {

		@Override
		public Tuple2<T, T> map(T vertex) {
			return new Tuple2<T, T>(vertex, vertex);
		}
	}

	@Override
	public String getDescription() {
		return "Parameters: <vertices-path> <edges-path> <result-path> <max-number-of-iterations>";
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String verticesPath = null;
	private static String edgesPath = null;
	private static String outputPath = null;
	private static int maxIterations = 10;
	private static String intermediateResultsPath = null;
	private static boolean parseParameters(String[] programArguments) {

		if(programArguments.length > 0) {
			// parse input arguments
			fileOutput = true;
			if(programArguments.length == 4) {
				edgesPath = programArguments[0];
				outputPath = programArguments[1];
				maxIterations = Integer.parseInt(programArguments[2]);
				intermediateResultsPath = programArguments[3];
			} else {
				System.err.println("Usage: ConnectedComponents <edges path> " +
						"<result path> <max number of iterations> <intermediate results path>");
				return false;
			}
		} else {
			System.out.println("Executing Connected Components example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: ConnectedComponents <edges path> " +
					"<result path> <max number of iterations> <intermediate results path>");
		}
		return true;
	}

	private static DataSet<Long> getVertexDataSet(ExecutionEnvironment env) {

		if(fileOutput) {
			return env.readCsvFile(verticesPath).types(Long.class)
					.map(
							new MapFunction<Tuple1<Long>, Long>() {
								public Long map(Tuple1<Long> value) { return value.f0; }
							});
		} else {
			return ConnectedComponentsData.getDefaultVertexDataSet(env);
		}
	}

	private static DataSet<Tuple2<Long, Long>> getEdgeDataSet(ExecutionEnvironment env) {

		if(fileOutput) {
			return env.readCsvFile(edgesPath).fieldDelimiter(" ").types(Long.class, Long.class);
		} else {
			return ConnectedComponentsData.getDefaultEdgeDataSet(env);
		}
	}



	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

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
	 * Function that initial the connected components with its own id.
	 */
	public static final class InitialValue implements GroupReduceFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

		@Override
		public void reduce(Iterable<Tuple2<Integer, Integer>> t, Collector<Tuple2<Integer, Integer>> c) throws Exception {
			Integer v = t.iterator().next().f0;
			c.collect(new Tuple2<Integer, Integer>(v, v));
		}
	}

	/**
	 * Undirected edges by emitting for each input edge the input edges itself and an inverted
	 * version.
	 */
	public static final class UndirectEdge implements FlatMapFunction<String, Tuple2<Integer, Integer>> {

		@Override
		public void flatMap(String edge, Collector<Tuple2<Integer, Integer>> out) {
			String[] line = edge.split("\t");
			Integer v1 = Integer.parseInt(line[0]);
			Integer v2 = Integer.parseInt(line[1]);
			out.collect(new Tuple2<Integer, Integer>(v1, v2));
			out.collect(new Tuple2<Integer, Integer>(v2, v1));
		}
	}



	/**
	 * UDF that joins a (Vertex-ID, Component-ID) pair that represents the current component that a
	 * vertex is associated with, with a (Source-Vertex-ID, Target-VertexID) edge. The function
	 * produces a (Target-vertex-ID, Component-ID) pair.
	 */
	@ForwardedFieldsFirst("f1->f1")
	@ForwardedFieldsSecond("f1->f0")
	public static final class NeighborWithComponentIDJoin implements
			JoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

		@Override
		public Tuple2<Integer, Integer> join(Tuple2<Integer, Integer> vertexWithComponent, Tuple2<Integer, Integer> edge) {
			return new Tuple2<Integer, Integer>(edge.f1, vertexWithComponent.f1);
		}
	}

	@ForwardedFieldsFirst("*")
	public static final class ComponentIdFilter implements
			FlatJoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

		@Override
		public void join(Tuple2<Integer, Integer> candidate, Tuple2<Integer, Integer> old, Collector<Tuple2<Integer, Integer>> out) {
			if (candidate.f1 < old.f1) {
				out.collect(candidate);
			}
		}
	}

	public static final class UpdateSolutionSet implements
			CoGroupFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

		@Override
		public void coGroup(Iterable<Tuple2<Integer, Integer>> ss, Iterable<Tuple2<Integer, Integer>> delta, Collector<Tuple2<Integer, Integer>> c) throws Exception {
			Iterator<Tuple2<Integer, Integer>> it1 = ss.iterator();
			Iterator<Tuple2<Integer, Integer>> it2 = delta.iterator();
			if (it2.hasNext()) {
				c.collect(it2.next());
			} else {
				c.collect(it1.next());
			}
		}
	}

}
