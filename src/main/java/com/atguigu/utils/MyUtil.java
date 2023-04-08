package com.atguigu.utils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class MyUtil {
    static LoadingCache<String, String> cache = CacheBuilder.newBuilder().expireAfterAccess(5, TimeUnit.MINUTES).build(
            new CacheLoader<String, String>() {
                @Override
                public String load(String key) throws Exception {
                    //User value = query(key);//from databse, disk, etc.
                    String value = "no" + key;
                    System.out.println("no" + key);
                    /*if ("a".equals(key)) {
                        cache.put("a", "av");
                    }*/

                    return value;
                }
            }
    );

    public static <T> List<T> toList(Iterable<T> it) {
        List<T> result = new ArrayList<>();
        for (T t : it) {
            result.add(t);
        }
        return result;
    }

    public static void main(String[] args) throws ExecutionException {
      /*  String a = cache.getIfPresent("a");
        a.equals("a");
        String unchecked = cache.getUnchecked("");
        cache.put("a", "av");
        System.out.println(a);
        System.out.println(a);*/
        /*byte[] encode = Base64.getEncoder().encode("Kingsoft.com123".getBytes());
        java.lang.String s = new java.lang.String(encode);
        System.out.println(s);
        String s2luZ3NvZnQuY29tMTIz = new String(Base64.getDecoder().decode("S2luZ3NvZnQuY29tMTIz"));
        System.out.println(s2luZ3NvZnQuY29tMTIz);
        Object obj = null;
        Boolean b;
        b = (Boolean) obj;
        System.out.println(b);
        if (b == true) {
            System.out.println(1);


        } else {
            System.out.println(2);
        }*/
        /*boolean equals = "184".equals(null);
        System.out.println(equals);*/
        Boolean equals = "184".equals(null);
        System.out.println(equals);

        String s2luZ3NvZnQuY29tMTIz = new String(Base64.getDecoder().decode("S2luZ3NvZnQuY29tMTIz"));
        System.out.println(s2luZ3NvZnQuY29tMTIz);
//        BufferedReader br = new BufferedReader(new InputStreamReader(System.in), ";");
        Scanner sc = new Scanner(System.in);
        sc.useDelimiter(";");
        String s = sc.nextLine();
    }
}