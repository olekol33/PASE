import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class ExperimentLauncher {
    private static final Logger logger = Logger.getLogger(ExperimentLauncher.class.getName());

    public static void main(String[] args) {
        try {
            if (args.length != 1) {
                logger.severe("Incorrect number of inputs: <type>");
                System.exit(1);
            }

            int type = Integer.parseInt(args[0]);
            HashMap<String, Object> config = new ConfigParser().getConfig();
            ExperimentWrapper expWrapper = new ExperimentWrapper(config);

            switch (type) {
                case 0:
                    expWrapper.runFullExperiment(ExperimentType.MispredRate);
                    break;
                case 2:
                    expWrapper.runFullExperiment(ExperimentType.SetupCost);
                    break;
                case 3:
                    expWrapper.runFullExperiment(ExperimentType.Utilization);
                    break;
                default:
                    logger.severe("Invalid experiment type");
                    System.exit(1);
            }
        } catch (Exception e) {
            System.out.println("Exception: " + e);
            e.printStackTrace();
        }
    }
}

enum ExperimentType {
    Utilization("utilization"),
    SetupCost("setup_cost"),
    MispredRate("mispred_rate"),
    Other("OTHER");

    private final String shortCode;

    ExperimentType(String shortCode) {
        this.shortCode = shortCode;
    }

    public String getName() {
        return shortCode;
    }
}


class ExperimentWrapper {
    private final HashMap<String, Object> config;
    private final FileHandler fileHandler;
    private Path logdirPath;
    private ExperimentType type;

    public ExperimentWrapper(HashMap<String, Object> config) {
        this.config = config;
        this.fileHandler = new FileHandler(config, null);
    }

    public void runFullExperiment(ExperimentType type) {
        this.type = type;
        logdirPath = this.fileHandler.createMainDir(type.getName());
        int iterations = (int) config.get("iterations");
        int seed = (int) config.get("seed");

        int availableProcessors = Runtime.getRuntime().availableProcessors();
        int maxThreads = Math.max(1, availableProcessors - 4);
        ExecutorService executor = Executors.newFixedThreadPool(maxThreads);

        for (int i = 0; i < iterations; ++i) {
            int currentSeed = i + seed;
            executor.submit(() -> {
                runSeedIteration(currentSeed);
            });
        }

        // Shut down the executor service
        executor.shutdown();

        try {
            // Wait for all tasks to finish
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void runSeedIteration(int seed){
        switch (type) {
            case Utilization:
                utilizationExp(seed);
                break;
            case SetupCost:
                setupCostExp(seed);
                break;
            case MispredRate:
                MisPredRateExp(seed);
                break;
            case Other:
                otherExp();
                break;
            default:
                throw new IllegalArgumentException("Invalid experiment type!");
        }
    }

    // Define the experiment methods
    private void utilizationExp(int seed) {
        new Experiment(seed, this.logdirPath, "dcUtilization", "Double", null, config);
    }

    private void setupCostExp(int seed) {
        System.out.println("Running setup cost experiment...");
        HashMap<String, Object> additionalParams = new HashMap<>();
        additionalParams.put("normalDistLatencyConstAppSize", true);
        new Experiment(seed, this.logdirPath, "stateCost", "Integer", additionalParams, config);
    }

    private void MisPredRateExp(int seed) {
        HashMap<String, Object> additionalParams = new HashMap<>();
        additionalParams.put("locationErrorModel", true);
        new Experiment(seed, this.logdirPath, "locationErrorRate","Double", additionalParams,config);
    }

    private void otherExp() {
        System.out.println("Running other experiment...");
        // Add your experiment logic here
    }

    private void setSeed(int seed, HashMap<String, Object> config){
        String logdir = "runlogs/" + seed;
        config.put("logdir", logdir);
    }
}

class Experiment{
    HashMap<String,Object> config;
    String mainParam;
    String mainParamType;
    Path aggRunsPath;
    FileHandler fileHandler;
    public Experiment(int seed, Path logdirPath, String mainParam, String mainParamType, HashMap<String, Object> additionalParams,
                      HashMap<String,Object> config) {
        this.config = config;
        this.mainParam = mainParam;
        this.mainParamType = mainParamType;
        this.aggRunsPath = logdirPath;
        setAdditionalParams(additionalParams);
        String[] values = getMainParamValuesFromConfig();
        for (String value : values) {
            updateConfig(value, seed);
            runExperiment(value);
        }
    }

    private String[] getMainParamValuesFromConfig(){
        if (config.containsKey("values"))
            return (String[]) config.get("values");
        else
            throw new IllegalArgumentException("No main param values for experiment");
    }

    private void setAdditionalParams(HashMap<String, Object> additionalParams){
        if(additionalParams==null)
            return;
        for(Map.Entry<String, Object> param : additionalParams.entrySet()){
            System.out.println("Set " + param.getKey() + " = " + String.valueOf(param.getValue()));
            config.put(param.getKey(), param.getValue());
        }

    }

    private void runExperiment(String value){
        this.fileHandler = new FileHandler(config, null);
        Path srcPath = fileHandler.getSeeddir();
        try {
            new OptimizationLauncher(config);
            Path dstPath = this.aggRunsPath.resolve(value);
            FileHandler.copyDirectory(srcPath, dstPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void updateConfig(String value, int seed){
        HashMap<String, Object> config_cp = new HashMap<>(this.config);
        if (mainParamType.equals("Integer"))
            config_cp.put(mainParam, Integer.valueOf(value));
        else if (mainParamType.equals("Double"))
            config_cp.put(mainParam, Double.valueOf(value));
        else
            throw new IllegalArgumentException("Value type not defined");
        config_cp.put("seed", seed);
        this.config = config_cp;
    }

}