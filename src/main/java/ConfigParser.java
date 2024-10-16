import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

// Remove JDOM imports

public class ConfigParser {

    HashMap<String, Object> config;

    public ConfigParser() throws IOException, SAXException, ParserConfigurationException {
        String configFile = "include/config.xml";
        config = new HashMap<>();
        CheckFileExists(configFile);

        // Using DOM parser instead of SAXBuilder
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.parse(new File(configFile));
        doc.getDocumentElement().normalize(); // Normalize the document

        Element rootNode = doc.getDocumentElement();

        // Replace JDOM's getChildren with DOM's getElementsByTagName
        NodeList runList = rootNode.getElementsByTagName("run");
        Node runNode = runList.item(0); // Assuming there is only one "run" element
        if (runNode.getNodeType() == Node.ELEMENT_NODE) {
            Element runElement = (Element) runNode;
            config.put("debugMode", Boolean.valueOf(getTagValue("debugMode", runElement)));
            config.put("deterministicModel", Boolean.valueOf(getTagValue("deterministicModel", runElement)));
            config.put("locationErrorModel", Boolean.valueOf(getTagValue("locationErrorModel", runElement)));
            config.put("applyLowRevenueHeuristic", Boolean.valueOf(getTagValue("applyLowRevenueHeuristic", runElement)));
            config.put("checkBrokenChains", Boolean.valueOf(getTagValue("checkBrokenChains", runElement)));
            config.put("limitUserReqs", Boolean.valueOf(getTagValue("limitUserReqs", runElement)));
            config.put("levyWalk", Boolean.valueOf(getTagValue("levyWalk", runElement)));
            config.put("end2endLatency", Boolean.valueOf(getTagValue("end2endLatency", runElement)));
            config.put("placeAtUserLoc", Boolean.valueOf(getTagValue("placeAtUserLoc", runElement)));
            config.put("nonSplitMode", Boolean.valueOf(getTagValue("nonSplitMode", runElement)));
            config.put("iterations", Integer.valueOf(getTagValue("iterations", runElement)));
            config.put("locationErrorRate", Double.valueOf(getTagValue("locationErrorRate", runElement)));
            config.put("fullTimeLimit", Integer.valueOf(getTagValue("fullTimeLimit", runElement)));
            config.put("initialRealisticCapacity", Integer.valueOf(getTagValue("initialRealisticCapacity", runElement)));
            config.put("heuristicGap", Double.valueOf(getTagValue("heuristicGap", runElement)));
            config.put("mamiThreshold", Double.valueOf(getTagValue("mamiThreshold", runElement)));
            config.put("algThreshold", Double.valueOf(getTagValue("algThreshold", runElement)));
            config.put("levyWalkExitRate", Double.valueOf(getTagValue("levyWalkExitRate", runElement)));
        }

        NodeList requestsList = rootNode.getElementsByTagName("requests");
        Node requestsNode = requestsList.item(0);
        if (requestsNode.getNodeType() == Node.ELEMENT_NODE) {
            Element requestsElement = (Element) requestsNode;
            config.put("syntheticRequests", Boolean.valueOf(getTagValue("syntheticRequests", requestsElement)));
            config.put("mergeProbabilitiesFile", Boolean.valueOf(getTagValue("mergeProbabilitiesFile", requestsElement)));
            config.put("revenueModel", Boolean.valueOf(getTagValue("revenueModel", requestsElement)));
            config.put("seed", Integer.valueOf(getTagValue("seed", requestsElement)));
            config.put("duration", Integer.valueOf(getTagValue("duration", requestsElement)));
            config.put("revenueMean", Integer.valueOf(getTagValue("revenueMean", requestsElement)));
            config.put("revenueStd", Integer.valueOf(getTagValue("revenueStd", requestsElement)));
            config.put("initialRequestsPerSec", Integer.valueOf(getTagValue("initialRequestsPerSec", requestsElement)));
            config.put("additionalRequestsPerSec", Integer.valueOf(getTagValue("additionalRequestsPerSec", requestsElement)));
            config.put("threshold", Double.valueOf(getTagValue("threshold", requestsElement)));
            config.put("numPredPerRequest", Integer.valueOf(getTagValue("numPredPerRequest", requestsElement)));
        }

        NodeList appsList = rootNode.getElementsByTagName("apps");
        Node appsNode = appsList.item(0);
        if (appsNode.getNodeType() == Node.ELEMENT_NODE) {
            Element appsElement = (Element) appsNode;
            config.put("syntheticApps", Boolean.valueOf(getTagValue("syntheticApps", appsElement)));
            config.put("normalDistApps", Boolean.valueOf(getTagValue("normalDistApps", appsElement)));
            config.put("zipfDistApps", Boolean.valueOf(getTagValue("zipfDistApps", appsElement)));
            config.put("normalDistLatencyConstAppSize", Boolean.valueOf(getTagValue("normalDistLatencyConstAppSize", appsElement)));
            config.put("numberOfApps", Integer.valueOf(getTagValue("numberOfApps", appsElement)));
            config.put("stdPairConstrains", Double.valueOf(getTagValue("stdPairConstrains", appsElement)));
            config.put("meanAppPairs", Double.valueOf(getTagValue("meanAppPairs", appsElement)));
            config.put("appPairsStd", Double.valueOf(getTagValue("appPairsStd", appsElement)));
            config.put("meanPairConstraint", Double.valueOf(getTagValue("meanPairConstraint", appsElement)));

            String specificAppSizes = getTagValue("specificAppSizes", appsElement);
            String[] specificAppSizesArray = specificAppSizes.split(",");
            int[] specificAppSizesIntArray = new int[specificAppSizesArray.length];
            for (int i = 0; i < specificAppSizesArray.length; i++) {
                specificAppSizesIntArray[i] = Integer.parseInt(specificAppSizesArray[i]);
            }
            config.put("specificAppSizes", specificAppSizesIntArray);
        }

        NodeList functionsList = rootNode.getElementsByTagName("functions");
        Node functionsNode = functionsList.item(0);
        if (functionsNode.getNodeType() == Node.ELEMENT_NODE) {
            Element functionsElement = (Element) functionsNode;
            config.put("syntheticFunctions", Boolean.valueOf(getTagValue("syntheticFunctions", functionsElement)));
            config.put("functionNormalDist", Boolean.valueOf(getTagValue("functionNormalDist", functionsElement)));
            config.put("numberOfFunctions", Integer.valueOf(getTagValue("numberOfFunctions", functionsElement)));
            config.put("meanFunctionSize", Integer.valueOf(getTagValue("meanFunctionSize", functionsElement)));
            config.put("meanLaunchTime", Double.valueOf(getTagValue("meanLaunchTime", functionsElement)));
        }

        NodeList dcList = rootNode.getElementsByTagName("dc");
        Node dcNode = dcList.item(0);
        if (dcNode.getNodeType() == Node.ELEMENT_NODE) {
            Element dcElement = (Element) dcNode;
            config.put("dcFromCoordinates", Boolean.valueOf(getTagValue("dcFromCoordinates", dcElement)));
            config.put("dcUtilization", Double.valueOf(getTagValue("dcUtilization", dcElement)));
            int stateCost = Integer.parseInt(getTagValue("stateCost", dcElement));
            config.put("stateCost", stateCost);
            config.put("capacityRatioThreshold", Double.valueOf(getTagValue("capacityRatioThreshold", dcElement)));
            config.put("dynamicStateCost", Boolean.valueOf(getTagValue("dynamicStateCost", dcElement)));
            config.put("dynamicStateCostStep", Integer.valueOf(getTagValue("dynamicStateCostStepPercent", dcElement)));
            config.put("capacity", Integer.valueOf(getTagValue("capacity", dcElement)));
        }

        NodeList linksList = rootNode.getElementsByTagName("links");
        Node linksNode = linksList.item(0);
        if (linksNode.getNodeType() == Node.ELEMENT_NODE) {
            Element linksElement = (Element) linksNode;
            config.put("allowedRange", Integer.valueOf(getTagValue("allowedRange", linksElement)));
            config.put("delay", Integer.valueOf(getTagValue("delay", linksElement)));
            config.put("bw", Integer.valueOf(getTagValue("bw", linksElement)));
        }

        NodeList batchRunList = rootNode.getElementsByTagName("batchRun");
        Node batchRunNode = batchRunList.item(0);
        if (batchRunNode.getNodeType() == Node.ELEMENT_NODE) {
            Element batchRunElement = (Element) batchRunNode;
            String values = getTagValue("values", batchRunElement);
            if (values != null && !values.isEmpty()) {
                config.put("values", values.split(","));
            }
            String optimizationRunType = getTagValue("optimizationRunType", batchRunElement);
            if (optimizationRunType != null && !optimizationRunType.isEmpty()) {
                config.put("optimizationRunType", optimizationRunType);
            }
            config.put("regularRun", Boolean.valueOf(getTagValue("regularRun", batchRunElement)));
            config.put("fullRun", Boolean.valueOf(getTagValue("fullRun", batchRunElement)));
            config.put("onlineRun", Boolean.valueOf(getTagValue("onlineRun", batchRunElement)));
            config.put("locationError", Boolean.valueOf(getTagValue("locationError", batchRunElement)));
            config.put("onlineFullRun", Boolean.valueOf(getTagValue("onlineFullRun", batchRunElement)));
            config.put("adversarialRun", Boolean.valueOf(getTagValue("adversarialRun", batchRunElement)));
            config.put("mamiRun", Boolean.valueOf(getTagValue("mamiRun", batchRunElement)));
        }
    }

    private String getTagValue(String tag, Element element) {
        NodeList nodeList = element.getElementsByTagName(tag);
        if (nodeList != null && nodeList.getLength() > 0) {
            Node node = nodeList.item(0);
            if (node != null && node.getNodeType() == Node.ELEMENT_NODE) {
                return node.getTextContent();
            }
        }
        return null;
    }

    private void CheckFileExists(String configFile) {
        File file = new File(configFile);
        if (!file.exists()) {
            throw new IllegalArgumentException("File " + configFile + " does not exist");
        }
    }

    public HashMap<String, Object> getConfig() {
        return config;
    }

}
