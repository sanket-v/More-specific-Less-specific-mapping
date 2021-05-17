//STEP 1. Import required packages
import java.sql.*;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

public class RunApplyAllFilters {
	// JDBC driver name and database URL
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	static final String DB_URL = "jdbc:mysql://localhost:3306";
	
	//  Database credentials
	static final String USER = "root";
	static final String PASS = "P@ssw0rd@123";
	
	public static void main(String[] args) throws Exception {
	Connection conn = null;
	Statement stmt = null;
	
	  //STEP 2: Register JDBC driver
	  Class.forName("com.mysql.jdbc.Driver");
	
	  //STEP 3: Open a connection
	  conn = DriverManager.getConnection(DB_URL, USER, PASS);

	  //STEP 4: Execute a query
	  stmt = conn.createStatement();
	  
//	  code prefixes that need to be mapped 
	  ArrayList<String> prefix_codes = readCodesFromPrefixOfCodesList_ForFilter();
	  HashSet<String> previous_knowledge = new HashSet<String>();
	  loadPreviouslyMappedKnoledge(previous_knowledge);
	  
//	  do iteration for every code prefix
	  for(String prefix_code : prefix_codes) {
		  System.out.println("Mapping code: "+prefix_code);
		  String sql = "SELECT Code, Description FROM ICD9_LOOKUP_ezNLP.codeMaster where billableFlag= 1 AND Code like '" + prefix_code + "%'";
		  ResultSet rs = stmt.executeQuery(sql);
		  
		  // writing result set to file
		  String code_desc_db_file_location = "/home/sanketv/Output of Eclipse projects/Output of MoreLessSpecific/Code Desc/"+prefix_code+"_code_desc_db.tsv";
		  BufferedWriter bw = new BufferedWriter(new FileWriter(code_desc_db_file_location));
		  PrintWriter out = new PrintWriter(bw);
		  
		  while(rs.next()){
		     out.println(rs.getString("Code")+"\t"+rs.getString("Description"));
		  }
		  // restultset processed
		  rs.close();
		  bw.close();
		  
		  // write to file
		  String special_symbol_removed = removeSpecialSymbol(prefix_code, code_desc_db_file_location);
		  String ignore_word_removed_location = writeRemoveIgnoreWords(prefix_code, special_symbol_removed);
		  String mapping_without_filter_file_location = mapCodesWithoutFilter(prefix_code, ignore_word_removed_location);
		  applyfilters(prefix_code, mapping_without_filter_file_location, previous_knowledge);
	  }
	  // all code prefixes processed
	  System.out.println("########################################");
	  System.out.println("Total codes mapped: "+prefix_codes.size());
	}
	private static void loadPreviouslyMappedKnoledge(HashSet<String> previous_knowledge) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader("/home/sanketv/eclipse_workspace/MoreLessSpecificFilter/resources/PreviosKnowledge.tsv"));
		String line = new String();
		while((line=br.readLine())!=null) {
			String arr[] = line.split("\\|");
			previous_knowledge.add(arr[0]+arr[1]);
		}
		br.close();
	}
	private static String removeSpecialSymbol(String prefix_code, String code_desc_db_file_location) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(code_desc_db_file_location));
		String line = new String();
		
		String output_file_location = "/home/sanketv/Output of Eclipse projects/Output of MoreLessSpecific/Special Symbols Removed/"+prefix_code+".tsv";
		BufferedWriter bw = new BufferedWriter(new FileWriter(output_file_location));
		PrintWriter out =  new PrintWriter(bw);
		
		while((line=br.readLine())!=null) {
			String arr[] = line.split("\t");
			String code = arr[0];
			String desc = arr[1];
			StringBuffer sb = new StringBuffer();
			for(int i=0;i<desc.length();i++) {
				if(Character.isLetter(desc.charAt(i)) || desc.charAt(i)==' ' || Character.isDigit(desc.charAt(i)))
					sb.append(desc.charAt(i));
			}
			out.println(code+"\t"+sb.toString());
		}
		bw.close();
		br.close();
		
		return output_file_location;
	}
	// main method complete
	
	private static String writeRemoveIgnoreWords(String prefix_code, String code_desc_db_file_location) throws Exception{
		Set<String> phrase = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
		phrase.add("and");
		phrase.add("without");
		phrase.add("with");
		
		//task given on 18 March, 2021 by Pinalbhai
		//phrase.add("other");
		phrase.add("specified");
		phrase.add("unspecified");
		phrase.add("complications");
		phrase.add("complication");
		phrase.add("unclassified");
		phrase.add("uncomplicated");
		
		// reading original file which id generated from database 
		BufferedReader br = new BufferedReader(new FileReader(code_desc_db_file_location));
		String line = new String();
		
		// writing code decription pair in file 
		String ignore_word_removed_location = "/home/sanketv/Output of Eclipse projects/Output of MoreLessSpecific/Ignore Words Removed/"+prefix_code+"_code_desc_phrases_removed.tsv";
		BufferedWriter bw = new BufferedWriter(new FileWriter(ignore_word_removed_location));
		PrintWriter out = new PrintWriter(bw);
		
		
		while((line=br.readLine())!=null) {
			String arr[] = line.split("\t");
			String code = arr[0];
			String desc = arr[1];

			String [] words = desc.split(" ");
			StringBuffer sb = new StringBuffer();
			for(String word : words){
				if(!phrase.contains(word))
					sb.append(word+" ");
			}
			out.println(code+"\t"+sb.toString());
		}
		bw.close();
		br.close();
		return ignore_word_removed_location;
	}


	private static String mapCodesWithoutFilter(String prefix_code, String ignore_word_removed_location) throws Exception  {		
		// code_desc_words map of codes and space(' ') sperated words in desc
		HashMap<String, HashSet<String>> code_desc_words = new HashMap<>();

		// code_list to traverse codes in source file
		ArrayList<String> code_list = new ArrayList<String>();

		// code_desc_phrases_removed.tsv file is generated by '../1. Remove Ignore Words/RemoveIgnoreWords.java'
		BufferedReader br = new BufferedReader(new FileReader(ignore_word_removed_location));
		String line = new String();

		// adding code_list and words of description in list and map
		while((line=br.readLine()) != null){
			String code = line.split("\t")[0];
			String []words = line.split("\t")[1].split(" ");
			HashSet<String> word_set = new HashSet<String>();
			for(String word : words)
				word_set.add(word.toLowerCase());
			code_list.add(code);
			code_desc_words.put(code, word_set);
		}

		br.close();

		// map of codes and original desc with phrases. This file includes igonre words
		HashMap<String, String> code_desc = new HashMap<>();
		
		
		String code_desc_file = "/home/sanketv/Output of Eclipse projects/Output of MoreLessSpecific/Code Desc/"+prefix_code+"_code_desc_db.tsv";
		br = new BufferedReader(new FileReader(code_desc_file));
		line = new String();
		while((line=br.readLine())!=null){
			code_desc.put(line.split("\t")[0], line.split("\t")[1]);
		}
		br.close();

		String mapping_without_filter_file_location = "/home/sanketv/Output of Eclipse projects/Output of MoreLessSpecific/Mapping Without Filter/"+prefix_code+"_more_less_specific.tsv";
		BufferedWriter bw = new  BufferedWriter(new FileWriter(mapping_without_filter_file_location));
		PrintWriter out = new PrintWriter(bw);

		for(int i=0;i<code_list.size();i++){
			// clone of words in desc of perticular code
			HashSet<String> primary = new HashSet<>(code_desc_words.get(code_list.get(i)));

			for (int j=0; j<code_list.size(); j++) {
				if(i==j)
					continue;

					// clone of words in desc of perticular code
					HashSet<String> compare_to = new HashSet<>(code_desc_words.get(code_list.get(j)));

					Iterator<String> it = primary.iterator();
					StringBuffer unique = new StringBuffer();
					
					while(it.hasNext()){
						String word = it.next();
						if(compare_to.contains(word))
							compare_to.remove(word);
						else
							unique.append(word+" ");
					}

					if(primary.size()>0 && compare_to.size()==0 && unique.length()>0)
						out.println(code_list.get(i)+"\t"+code_desc.get(code_list.get(i))+"\t"+code_list.get(j)+"\t"+code_desc.get(code_list.get(j))+"\t"+unique.toString());
					
					// task given by pratik bhai on May 13, 2021
					// consider code as more specific if its description contains 'bilateral' and less specfic if it contains 'left' or 'right'
					if(primary.contains("bilateral") && compare_to.size()<=2 && (compare_to.contains("right") || compare_to.contains("left")) && unique.length()>0) {
						if(compare_to.size()==1 && (compare_to.contains("right") || compare_to.contains("left"))) {
							System.out.println(code_list.get(i)+" <--> "+code_list.get(j)+" --> "+primary+" <--> "+compare_to);
							out.println(code_list.get(i)+"\t"+code_desc.get(code_list.get(i))+"\t"+code_list.get(j)+"\t"+code_desc.get(code_list.get(j))+"\t"+unique.toString());
						}
						if(compare_to.size()==2 && (compare_to.contains("right") || compare_to.contains("left")) && (compare_to.contains("eye") || compare_to.contains("leg") || compare_to.contains("hand"))) {
							System.out.println(code_list.get(i)+" <--> "+code_list.get(j)+" --> "+primary+" <--> "+compare_to);
							out.println(code_list.get(i)+"\t"+code_desc.get(code_list.get(i))+"\t"+code_list.get(j)+"\t"+code_desc.get(code_list.get(j))+"\t"+unique.toString());
						}
					}
				}
			}
			bw.close();
			return mapping_without_filter_file_location;
	}
	private static void applyfilters(String prefix_code, String mapping_without_filter_file_location, HashSet<String> previous_knowledge) throws Exception{
		String filter_1_mapping_location = filterPricipalSymptom(prefix_code, mapping_without_filter_file_location, previous_knowledge);
		String filter_2_mapping_location = filterMoreSpecific(prefix_code, filter_1_mapping_location, previous_knowledge);
		String filter_3_mapping_location = filterLessSpecific(prefix_code, filter_2_mapping_location, previous_knowledge);
		CheckForUnspecified filter_5 = new CheckForUnspecified();
		filter_5.applyFilter(prefix_code, filter_3_mapping_location, previous_knowledge);
	}
	
	private static String filterLessSpecific(String prefix_code, String filter_2_mapping_location, HashSet<String> previous_knowledge) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader("/home/sanketv/eclipse_workspace/MoreLessSpecificFilter/resources/LessSpecificCodes.tsv"));
        String line = new String();

        HashSet<String> less_spec_set = new HashSet<>();

        while((line=br.readLine())!=null){
            String [] arr = line.split("\t");
            String more_less_mapping = arr[0]+arr[1];
            less_spec_set.add(more_less_mapping);
        }

        br.close();

        br = new BufferedReader(new FileReader(filter_2_mapping_location));
        line = new String();

        String filter_3_file_location = "/home/sanketv/Output of Eclipse projects/Output of MoreLessSpecific/Filter - 3 LessSpecific/"+prefix_code+".tsv";
        BufferedWriter bw = new BufferedWriter(new FileWriter(filter_3_file_location));
        PrintWriter out = new PrintWriter(bw);

        while((line = br.readLine())!=null){
            String arr[] = line.split("\t");

            if(!less_spec_set.contains(arr[0]+arr[2]) && !previous_knowledge.contains(arr[0]+arr[2]))
                out.println(line);
        }
        bw.close();
        br.close();
        return filter_3_file_location;
	}
	private static String filterMoreSpecific(String prefix_code, String filter_1_mapping_loaction, HashSet<String> previous_knowledge) throws Exception {
		// actual mapping of codes. not range of codes
        BufferedReader br = new BufferedReader(new FileReader("/home/sanketv/eclipse_workspace/MoreLessSpecificFilter/resources/MoreSpecificCodeRangeMapping.tsv"));
        String line = new String();
        String code_string = prefix_code;

        HashSet<String> more_less_from_filter_1_set = new HashSet<>();

        while((line = br.readLine())!=null){
            String arr[] = line.split("\t");
            if(line.indexOf(code_string)>=0 && line.indexOf(code_string)>=0)
                more_less_from_filter_1_set.add(arr[0]+arr[1]);
        }
        br.close();

        br = new BufferedReader(new FileReader(filter_1_mapping_loaction));
        line = new String();

        String filtered_file_location = "/home/sanketv/Output of Eclipse projects/Output of MoreLessSpecific/Filter - 2 More Specific/"+prefix_code+".tsv";
        BufferedWriter bw = new BufferedWriter(new FileWriter(filtered_file_location));
        PrintWriter out = new PrintWriter(bw);

        while((line=br.readLine())!=null){
            String [] arr = line.split("\t");
            String more_less_from_filter = arr[0]+arr[2];
            if(!more_less_from_filter_1_set.contains(more_less_from_filter) && !previous_knowledge.contains(more_less_from_filter))
                out.println(line);
        }
        bw.close();
        br.close();
        return filtered_file_location;
	}
	private static String filterPricipalSymptom(String prefix_code, String mapping_without_filter_file_location, HashSet<String> previous_knowledge) throws Exception {
		// PrincipalSymptomMap is actual mapping of codes. It is not code range (resource file) 
        BufferedReader br = new BufferedReader(new FileReader("/home/sanketv/eclipse_workspace/MoreLessSpecificFilter/resources/PrincipalSymptomMap.tsv"));
        String line = new String();

        HashSet<String> resource_map_pair_set = new HashSet<>();

        while((line=br.readLine())!=null){
            String arr[] = line.split("\t");
            if(arr[0].indexOf(prefix_code)>=0 && arr[1].indexOf(prefix_code)>=0)
                resource_map_pair_set.add(arr[0]+arr[1]);
        }

        br.close();

        br = new BufferedReader(new FileReader(mapping_without_filter_file_location));
        line = new String();
        boolean first_row = true;

        String filter_file_output_location = "/home/sanketv/Output of Eclipse projects/Output of MoreLessSpecific/Filter - 1 PrincipalSymptom/"+prefix_code+".tsv";
        BufferedWriter bw = new BufferedWriter(new FileWriter(filter_file_output_location));
        PrintWriter out = new PrintWriter(bw);

        while((line=br.readLine())!=null){
            if(first_row){
                first_row = false;
                out.println(line);
                continue;
            }
            String arr[] = line.split("\t");
            if(!resource_map_pair_set.contains(arr[0]+arr[2]) && !previous_knowledge.contains(arr[0]+"\t"+arr[2])){
                out.println(line);
            }
        }
        bw.close();
        br.close();
        return filter_file_output_location;
	}
	private static ArrayList<String> readCodesFromPrefixOfCodesList_ForFilter() throws Exception{
		ArrayList<String> res = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new FileReader("/home/sanketv/eclipse_workspace/MoreLessSpecificFilter/resources/PrefixOfCodesList_ForFilter.txt"));
		String line = new String();
		while((line=br.readLine())!=null) {
			res.add(line);
		}
		br.close();
		return res;
	}
}