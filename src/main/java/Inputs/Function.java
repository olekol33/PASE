package Inputs;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;

import java.util.HashMap;

public class Function {
    int id;
    int procSize;
    private static final int ID=0;
    private static final int SIZE=1;

    public Function(String[] function){
        id = Integer.parseInt(function[ID]);
        procSize = Integer.parseInt(function[SIZE]);
    }

    public int getId() {
        return id;
    }

    public int getProcSize() {
        return procSize;
    }

    /**
     * Synthetic generation of functions based on config
     * @param config input config
     */
    public static HashMap<Integer, Function> genFunctions(HashMap<String,Object> config){
        HashMap<Integer,Function> functions = new HashMap<>();
        int seed = (int)config.get("seed");

        int numberOfFunctions = (int) config.get("numberOfFunctions");
        int meanFunctionSize = (int) config.get("meanFunctionSize");

        RandomGenerator rand = new Well19937c((seed));
        for (int f = 1; f <= numberOfFunctions; f++) {
            if ((boolean)config.get("functionNormalDist")) {
                NormalDistribution normalSize = new NormalDistribution(rand, meanFunctionSize, 1);
                String[] function = new String[3];

                int size = (int) normalSize.sample();
                while (size <= 0)
                    size = (int) normalSize.sample();


                function[0] = String.valueOf(f);
                function[1] = String.valueOf(size);

                Function func = new Function(function);
                functions.put(func.getId(), func);
            }
            else{
                String[] function = new String[3];
                function[0] = String.valueOf(f);
                function[1] = String.valueOf(meanFunctionSize);
                Function func = new Function(function);
                functions.put(func.getId(), func);
            }
        }
        return functions;
    }
}
