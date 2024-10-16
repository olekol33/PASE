import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.Comparator;
import java.util.HashMap;

public class FileHandler {
    HashMap<String, Object> config;
    Path seedDir;
    Path logsDir = null;
    Path dumpDir = null;
    Path requestsDir = null;
    public FileHandler(HashMap<String, Object> _config, String expType) {
        config = _config;
        int seed = (int)config.get("seed");
        seedDir = Paths.get("runlogs/" + seed);
        if (expType != null) {
            logsDir = Paths.get(seedDir + "/logs_" + expType);
            dumpDir = Paths.get(logsDir + "/dump");
            requestsDir = Paths.get(dumpDir + "/requests");
            createRundirs();
        }
    }

    private void createRundirs(){
        try {
            if(Files.exists(logsDir))
                Files.walk(logsDir)
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            Files.createDirectories(logsDir);
            Files.createDirectories(dumpDir);
            Files.createDirectories(requestsDir);
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    public Path getSeeddir(){
        return seedDir;
    }

    public Path getLogdir(){
        return logsDir;
    }

    public Path getDumpdir(){
        return dumpDir;
    }

    public Path createMainDir(String expName){
        Path p = Paths.get(expName + "_runlogs");
        try {
            if (Files.exists(p))
                FileUtils.deleteDirectory(p.toFile());
            Files.createDirectories(p);
        }
        catch (IOException e){
            e.printStackTrace();
        }
        return p;
    }

    public static void copyDirectory(Path sourceDir, Path destinationDir) throws IOException {
        // Compute the target directory by including the source directory name under the destination
        Path targetDir = destinationDir.resolve(sourceDir.getFileName());

        // Create the top-level target directory (source directory under destination)
        Files.createDirectories(targetDir);

        // Copy the contents of the source directory into the new target directory
        try (DirectoryStream<Path> dir = Files.newDirectoryStream(sourceDir)) {
            for (Path srcPath : dir) {
                Path targetPath = targetDir.resolve(srcPath.getFileName());
                if (Files.isDirectory(srcPath)) {
                    // Recursively copy subdirectories
                    copyDirectory(srcPath, targetPath.getParent());
                } else {
                    // Copy files
                    Files.copy(srcPath, targetPath, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }

}
