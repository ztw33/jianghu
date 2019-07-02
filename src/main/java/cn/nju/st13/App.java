package cn.nju.st13;

import com.sun.tools.javac.util.StringUtils;
import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

public class App 
{
    public static void main( String[] args )
    {
        StringTokenizer itr = new StringTokenizer("1 2 3 4");
        while (itr.hasMoreTokens()) {
            itr.nextToken();
            System.out.println("a");
        }
    }
}
