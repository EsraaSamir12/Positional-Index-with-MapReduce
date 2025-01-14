package ir_project_;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;
public class reducer extends Reducer<Text, Text, Text, Text> {
    private Text positions = new Text();
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, List<String>> docPositionsMap = new TreeMap<>(new Comparator<String>() {
            @Override
            public int compare(String document1, String document2) {
                
                int number1 = Integer.parseInt(document1.replaceAll("\\D", ""));
                int number2 = Integer.parseInt(document2.replaceAll("\\D", ""));
                return Integer.compare(number1, number2);
            }
        });
        for (Text val : values) {
            String document_Information = val.toString(); 
            String[] document_parts = document_Information.split(":");
            String document_ID = document_parts[0];  
            String position = document_parts[1];  

            if (!docPositionsMap.containsKey(document_ID)) {
            	docPositionsMap.put(document_ID, new ArrayList<String>());
            }
            docPositionsMap.get(document_ID).add(position);
        }    
        int cnt=docPositionsMap.size();
        StringBuilder positionList = new StringBuilder();
        for (Map.Entry<String, List<String>> val : docPositionsMap.entrySet()) {
            String DOC_ID = val.getKey();
            List<String> positionsList = val.getValue();
            Collections.sort(positionsList);
            StringBuilder joined_Pos = new StringBuilder();
            for (String pos : positionsList) {
                if (joined_Pos.length() > 0) {
                	joined_Pos.append(", ");
                }
                joined_Pos.append(pos);
            }
            positionList.append(DOC_ID).append(": ");
            positionList.append(joined_Pos.toString()).append("; ");
        }
        positions.set(cnt +";"+positionList.toString().trim());
        context.write(key, positions);
    }
}
