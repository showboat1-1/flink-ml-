package cn.swust.algorithms.apriori;

import cn.hutool.core.collection.CollectionUtil;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

public class AprioriDemoTest {

    @Test
    public void main() {
        String[] strings = Arrays.asList("A C D", "B C E", "A B C E", "B E").toArray(new String[0]);
        for (String s : strings) {
            System.out.println(s);
        }
    }
    @Test

    public void hutoolTest() {
//        List<String> list1 = Arrays.asList("A", "B", "C", "D", "E", "F", null);
//        List<String> list2 = Arrays.asList("1", "2", "3", "D", "E", "F", null);
//        System.out.println("交集：" + CollectionUtil.intersection(list1, list2)); // 交集
//        System.out.println("补集：" + CollectionUtil.disjunction(list1, list2)); // 补集
//        System.out.println("并集：" + CollectionUtil.union(list1, list2)); //并集
//        System.out.println("list1的差集"+CollectionUtil.subtract(list1,list2));
//        System.out.println("list2的差集"+CollectionUtil.subtract(list2,list1));
//        System.out.println("list1的差集：" + CollectionUtil.subtractToList(list1, list2));
//        System.out.println("list2的差集：" + CollectionUtil.subtractToList(list2, list1));

        String str = "abc/wwj,sial.cjwj com/fahio&swnukb|dhauiw%fbauk$dhauiw;王二，牛奶 李四";
        String[] split = str.split("[\\u4e00-\\u9fa5_a-zA-Z0-9]+");
        for (String s : split) {
            System.out.println(s);
        }
        List<String> content = new ArrayList<>();
        Pattern pattern = Pattern.compile("[\\u4e00-\\u9fa5_a-zA-Z0-9]+");
        Matcher matcher = pattern.matcher(str);
        while (matcher.find()) {
            content.add(matcher.group());
        }
        String[] ss = content.toArray(new String[0]);
        System.out.println(ss.length);
    }
}