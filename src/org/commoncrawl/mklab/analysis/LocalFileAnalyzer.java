package org.commoncrawl.mklab.analysis;

import java.io.*;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by kandreadou on 11/14/14.
 */
public class LocalFileAnalyzer {

    private static final Set<String> s = new TreeSet<String>();
    private static final String DUMMY = "Image url distribution for host: ";
    public static void main(String[] args) throws Exception {

        PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter("/home/kandreadou/Desktop/news_images.txt", false)));

        BufferedReader reader = new BufferedReader(new InputStreamReader((new FileInputStream("/home/kandreadou/Desktop/news_distribution.txt")), "UTF-8"));
        String line = reader.readLine();
        while (line != null ) {
            if(line.startsWith(DUMMY)){
                String hostname = line.substring(DUMMY.length());
                s.add(hostname);
            }else{
                String[] elements = line.split("\\s+");
                int frequency = Integer.parseInt(elements[1]);
                if(frequency>20){
                    String hostname = elements[0];
                    if (hostname.startsWith("www.")) {
                        hostname = hostname.substring(4);
                    }
                    s.add(hostname);
                }
            }
            line = reader.readLine();
        }
        reader.close();

        for (String st: s){
            writer.println(st);
        }
        writer.close();
    }
}
