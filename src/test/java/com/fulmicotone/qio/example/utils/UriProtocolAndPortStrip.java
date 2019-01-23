package com.fulmicotone.qio.example.utils;

import java.util.function.Function;

/**
 * Created by dino on 27/07/16.
 */
public class UriProtocolAndPortStrip implements Function<String,String> {


    private static final String regex = "^(http://www\\." +
            "|http://|www\\." +
            "|https://www\\." +
            "|https://|www\\." +
            "|https://\\." +
            "|http://\\." +
            "|www.\\.)";

    private static final String isNumericRegex = "-?\\d+(\\.\\d+)?";

    @Override
    public String apply(String uri) {


        uri=uri
                .replaceFirst(regex,"");

        int portstart=uri.indexOf(":");

        if(portstart!=-1&&isNumeric(uri.charAt(portstart+1)+"")){
            int firstSlash=uri.indexOf("/",portstart);
            uri=firstSlash==-1?uri.substring(0,portstart):uri.substring(0,portstart)+uri.substring(firstSlash);
        }
        return uri;
    }


    private static boolean isNumeric(String str)
    {
        return str.matches(isNumericRegex);  //match a number with optional '-' and decimal.
    }


}
