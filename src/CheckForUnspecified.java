import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;

public class CheckForUnspecified {
    private HashMap < String, Integer > unspecifiedCodeMap;
    static int total_number_of_mapping = 0;
    CheckForUnspecified() {
        unspecifiedCodeMap = new HashMap < String, Integer > ();
    }

    void loadUnspecifiedCodes() throws IOException {
        String line = "";
        BufferedReader br = new BufferedReader(new FileReader("/home/sanketv/git/repezArogya/repezArogya/Arogya/resources/icd910Resourse/CodeFilter/UnspecifiedCodes"));
        while ((line = br.readLine()) != null) {
            if (line.startsWith("#"))
                continue;
            String[] part = line.split("\t");
            String code = part[0];
            String desc = part[1];
            int unspecifiedRank = calculateUnspecifiedRank(desc);
            unspecifiedCodeMap.put(code, unspecifiedRank);
        }
        br.close();
    }

    private int calculateUnspecifiedRank(String desc) {
        int rank = 0;
        desc = desc.toLowerCase();
        String descToHandleStarEndCase = "Start_" + desc + "_End";
        String[] part = descToHandleStarEndCase.split("unspecified");
        rank = part.length - 1;

        if (desc.contains("not elsewhere classified"))
            rank++;
        if (desc.contains("not classified"))
            rank++;
        if (desc.contains("not otherwise specified"))
            rank++;
        if (desc.endsWith("not specified"))
            rank++;
        if (desc.endsWith(" nec"))
            rank++;
        if (desc.matches("^.*?\\bnos\\b.*?$"))
            rank++;

        return rank;
    }

    void applyFilter(String prefix_code, String filter_3_mapping_location, HashSet<String> previous_knowledge) throws IOException {
        loadUnspecifiedCodes();
        BufferedReader br = new BufferedReader(new FileReader(filter_3_mapping_location));
        String line = new String();

        String filter_4_file_location = "/home/sanketv/Output of Eclipse projects/Output of MoreLessSpecific/Filter - 4 Unspecified/" + prefix_code + ".tsv";
        BufferedWriter bw = new BufferedWriter(new FileWriter(filter_4_file_location));
        PrintWriter out = new PrintWriter(bw);

        while ((line = br.readLine()) != null) {
            String arr[] = line.split("\t");
            // System.out.println(arr[0]+"<-->"+arr[2]+" --> "+isValidPair(arr[0], arr[2]));
            if (isValidPair(arr[0], arr[2]) && !previous_knowledge.contains(arr[0]+arr[2])) {
            	total_number_of_mapping++;
                out.println(line);
            }
        }

        bw.close();
        br.close();
        
        System.out.println("Total mappings: "+total_number_of_mapping);
    }

    private boolean isValidPair(String more, String less) {
        boolean isValidEntry = true;
        String unSpeCode = less;
        String speCode = more;
        if (isUnspecifiedCode(unSpeCode)) {
            if (!unSpeCode.equals(speCode)) {
                if (!isUnspecifiedCode(speCode)) {
                    String preUnSpeCode = unSpeCode.substring(0, unSpeCode.length() - 1);
                    if (speCode.contains(preUnSpeCode) && speCode.length() == unSpeCode.length())
                        isValidEntry = false;
                    else if (speCode.contains(preUnSpeCode) && preUnSpeCode.endsWith("."))
                        isValidEntry = false;
                } else if (isUnspecifiedCode(speCode)) {
                    int specRank = getUnspecifiedRannk(speCode);
                    int unSpecRank = getUnspecifiedRannk(unSpeCode);
                    String preUnSpeCode = unSpeCode.substring(0, unSpeCode.length() - 1);
                    if (speCode.length() == unSpeCode.length() && speCode.contains(preUnSpeCode) && (specRank < unSpecRank))
                        isValidEntry = false;
                }
            }
        }
        return isValidEntry;
    }

    private boolean isUnspecifiedCode(String unSpeCode) {
        return unspecifiedCodeMap.containsKey(unSpeCode);
    }

    private int getUnspecifiedRannk(String code) {
        return unspecifiedCodeMap.get(code);
    }
}