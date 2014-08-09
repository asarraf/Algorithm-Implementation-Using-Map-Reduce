// ANKIT SARRAF
// Mapper Class

package matrixmultiplication;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixMultiplicationMapper extends
Mapper<Object, Text, Text, Text> {
	private ArrayList<ArrayList<Integer>> matrixA = new ArrayList<ArrayList<Integer>>();
	private ArrayList<ArrayList<Integer>> matrixB = new ArrayList<ArrayList<Integer>>();

	private int matAi = 0;
	private int matBj = 0;

	private String currentMatrix = "";

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// Get each key word in the Document that is being read
		String [] matrixValues = value.toString().split("\\s+");

		ArrayList<Integer> eachRow = new ArrayList<Integer>();
		if(!matrixValues[0].equals("MatrixA") && !matrixValues[0].equals("MatrixB")) {
			for(String matrixValue: matrixValues) {
				eachRow.add(Integer.parseInt(matrixValue));
			}

			if(currentMatrix.equals("MatrixA")) {
				matrixA.add(eachRow);
				matAi++;

				if(matAi == Constants.DIMENSIONS) {
					// Emit Matrix A Values
					for(int i = 0 ; i < Constants.DIMENSIONS ; i++) {
						for(int j = 0 ; j < Constants.DIMENSIONS ; j++) {
							for(int k = 0 ; k < Constants.DIMENSIONS ; k++) {
								// Now the MatrixA is filled with all Values - Time to Emit
								String matAKey =
										new String("(" + i + "," + k + ")");
								String matAValue =
										new String("(" + j + "," + matrixA.get(i).get(j) + ")");

								System.out.println("Emiting A : <" + matAKey + "," + matAValue + ">");
								context.write(new Text(matAKey), new Text(matAValue));
							}
						}
					}
				}
			} else if(currentMatrix.equals("MatrixB")) {
				matrixB.add(eachRow);
				matBj++;

				if(matBj == Constants.DIMENSIONS) {
					// Emit Matrix B Values
					for(int j = 0 ; j < Constants.DIMENSIONS ; j++) {
						for(int k = 0 ; k < Constants.DIMENSIONS ; k++) {
							for(int i = 0 ; i < Constants.DIMENSIONS ; i++) {
								// Now the MatrixB is filled with all Values - Time to Emit
								String matBKey =
										new String("(" + i + "," + k + ")");
								String matBValue =
										new String("(" + j + "," + matrixB.get(j).get(k) + ")");

								System.out.println("Emiting B : <" + matBKey + "," + matBValue + ">");
								context.write(new Text(matBKey), new Text(matBValue));
							}
						}
					}
				}
			}
		} else {
			if(matrixValues[0].equals("MatrixA")) {
				currentMatrix = new String("MatrixA");
			} else {
				currentMatrix = new String("MatrixB");
			}
		}
	}
}