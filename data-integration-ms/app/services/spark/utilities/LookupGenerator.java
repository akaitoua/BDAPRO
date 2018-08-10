package services.spark.utilities;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;

/**
 * produces string permutations by deletions with the specific edit distance.
 * Permutations are word based (sentence is split into words and permutations are created per word) to avoid explosion.
 */
public class LookupGenerator {
    private static double byteCount = 0;

    public static void main(String[] args) throws Exception {

        String rootPath = "/mnt/work/code-base/IntegrationMicroService/";
        String output= "/home/venkat/Desktop/symspell.txt";
        String input2 = rootPath + "company_entities.csv";
        String input1 = rootPath + "company_profiles.csv";

        // build permutations hash for the smaller dataset
        Multimap<String, String> perms = buildHash(input1);

        Formatter fs= new Formatter(output);

        // TODO: read line by line from the second dataset
        String line;
        BufferedReader br = new BufferedReader(new FileReader(new File(input2)));
        while ((line = br.readLine()) != null) {
            getProbableMatches(line,perms);
        }
        System.out.println("DONE");
    }

    public static Multimap<String,String> buildHash(String input) {
        Multimap<String, String> perms = ArrayListMultimap.create();
        String line;

        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(input)));
            while ((line = br.readLine()) != null) {
                addMapToMultimap(buildPermutations(line,true), perms);
            }
        }catch(Exception ex)    {
            ex.printStackTrace();
        }
        return perms;
    }


    public static ArrayList<String> getProbableMatches(String line,Multimap<String, String> perms){

        ArrayList<String> possibleDups = new ArrayList<>();
//       Formatter fs= new Formatter(output);
        // store all duplicates resulted from the permutations with the word as key
        Multimap<String, String> duplicatePerWord = HashMultimap.create();
        String[] lineWords = line.split("\\t");
        String rowId = lineWords[0];
        // parse name column and split the column into words and then generate permuations per word
        String[] words = lineWords[1].toLowerCase().split("\\s+");
        for (String word : words) {
            Set<String> permsdup = buildPermutations(word, false).keySet();

            for (String s : permsdup) {
                if (perms.get(s).size() >= 1) {
                    duplicatePerWord.putAll(word, perms.get(s));
                    // System.out.println("word:"+ word + "  perm: "+ s +"  listIds: "+perms.get(s));
                }
            }
        }

        Collection<String> absDuplicates = null;

        // retain duplicates occured based on common occurance in all words
        // TODO: permutations if word not occured in all the parts
        for (String word : words) {
            if (absDuplicates == null) {
                absDuplicates = duplicatePerWord.get(word);
            } else {
                absDuplicates.retainAll(duplicatePerWord.get(word));
            }
        }

        Iterator<String> it = absDuplicates.iterator();
//        if (absDuplicates.size()>0) {
//            possibleDups.add(rowId);
//        }
        while (it.hasNext()) {
//            System.out.println(rowId+" "+it.next());
            possibleDups.add(it.next());
        }
        return possibleDups;
    }

    // TODO: build Permutation map for all the data
    // TODO: Add multival map, example sun,sin
    private static Map<String, String> buildPermutations(String in,boolean lineNumberPresent) {
        // "Porzellaneum Studentenheimverein der Wiener Universität";
        int edit = 2;
        String rowId="0";
        in=in.toLowerCase();
        if (!(in.length()>=1)){
            return  null;
        }
        String[] splitName;
        Map<String, String> inp = new HashMap();
        if (lineNumberPresent) {
            String[] inputSplit = in.split("\\t+");
            rowId = inputSplit[0];
            splitName = inputSplit[1].split("\\s+");
            inputSplit = null;
        }else {
            splitName = in.split("\\s+");
        }
        for (String s : splitName){ inp.put(s, rowId);}


        Map<String, String> permutations = new HashMap<>();
        // initialize with single edit
        Map<String, String> editKeys = edits(inp.keySet(), rowId);
        permutations.putAll(editKeys);
//        editKeys.forEach(va-> System.out.println(va));
        // single edit is done during initialization
        for (int i = 1; i < edit; i++) {
            Map<String, String> editKeys1 = edits(editKeys.keySet(), rowId);
            editKeys = editKeys1;
            permutations.putAll(editKeys);
        }
//        System.out.printf("string with %d bytes has %d permutations with total MB: %.2f\n", in.length() * 2,
//                permutations.size(), (byteCount * 2 / 1000000));
        return permutations;
    }

    private static HashMap<String, String> edits(Set<String> edits, String rowId) {
        HashMap<String, String> ret = new HashMap<>();
        StringBuilder sb = new StringBuilder();

        Iterator<String> st = edits.iterator();

        while (st.hasNext()) {
            String val = st.next();
            if (val.length() > 3 ) {
                for (int i = 0; i < val.length(); i++) {
                    sb.append(val).deleteCharAt(i);
                    String perm = sb.toString();
                    ret.put(perm, rowId);
                    sb.setLength(0);
                    byteCount += perm.length();
                }
            }
        }

        return ret;
    }

    private static void addMapToMultimap(Map<String,String> map,Multimap<String, String> mp){
        if (map==null){
            return;
        }
        for (String key : map.keySet()) {
            mp.put(key, map.get(key));
        }
    }
}
