package com.fulmicotone.qio.example.utils;

import com.google.common.net.InternetDomainName;

import java.util.Optional;
import java.util.function.Function;


public class DomainExtractor
        implements Function<String, Optional<String>> {

    @Override
    public Optional<String> apply(String uri) {

        try {
            uri=new UriProtocolAndPortStrip().apply(uri);

            int directoryIndex=uri.indexOf("/");
            int questionMarkIndex=uri.indexOf("?");

            if(directoryIndex!=-1){
                uri=uri.substring(0,directoryIndex);
            }

            if(directoryIndex == -1 && questionMarkIndex!=-1){
                uri=uri.substring(0,questionMarkIndex);
            }

            String uriParsed = InternetDomainName.from(uri).toString();

            return Optional.ofNullable(uriParsed);

        }catch (Exception e){return Optional.empty();}
    }

}
