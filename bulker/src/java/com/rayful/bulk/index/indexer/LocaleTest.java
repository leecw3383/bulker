package com.rayful.bulk.index.indexer;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;
 
public class LocaleTest {
    public static void main(String[] args) {
        Locale[] list = DateFormat.getAvailableLocales();
 
        ArrayList<String> aryLocale = new ArrayList<String>();
 
        for (int i = 0; i < list.length; i++) {
            aryLocale.add(list[i].toString());
        }
 
        Collections.sort(aryLocale);
 
        Locale locale = null;
        String[] arrData = null;
 
        for (String data : aryLocale) {
            if (data.indexOf("_") > -1) {
                arrData = data.split("_");
                locale = new Locale(arrData[0], arrData[1]);
            } else {
                locale = new Locale(data);
            }
 
            System.out.println(locale.toString() + "	"
                    + locale.getISO3Language() + "	"
                    + locale.getISO3Country() + "	["
                    + locale.getLanguage() + ","
                    + locale.getCountry() + "]	"
                    + locale.getDisplayLanguage() + "	"
                    + locale.getDisplayCountry()
                    );
        }
 
        Locale localeTest = new Locale("en", "US");
        System.out.println(">>>>>>>>>" + localeTest.getDisplayName());
    }
}