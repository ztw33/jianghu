package cn.nju.st13;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;

import java.util.List;

public class App 
{
    public static void main( String[] args )
    {
        Result result = ToAnalysis.parse("让战士们过一个欢乐祥和的新春佳节。");
        System.out.println(result.getTerms());
    }
}
