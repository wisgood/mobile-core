package com.bi.mobile.evideo.format.correctdata;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.bi.mobile.evideo.format.dataenum.EvideoOutVIDFormatEnum;

/*From 30/niewf
vid_column=8

if ( $field[4] =~ /(\d{1,3}\.){3}\d{1,3}/ ) {
    if ( $field[6] =~ /^[a-z]{3,}$/ ) {
        $vid = $field[ $vid_column - 1 ];
    } else {
        $vid = $field[ $vid_column - 2 ];
    }
} else{
    $vid = $field[ $vid_column - 2 ];
}
*/

public class getVIDUtil {
	
	public static String getVID(String[] splitSts) throws ArrayIndexOutOfBoundsException{
	    String vid="";
		String verRex = "(\\d{1,3}\\.){3}\\d{1,3}";
		String typeRex = "(^[a-z]{3,}$)";
		Pattern verPattern = Pattern.compile(verRex);
		Matcher verMatcher = verPattern.matcher(splitSts[EvideoOutVIDFormatEnum.VER.ordinal()]);
		Pattern typePattern = Pattern.compile(typeRex);
		Matcher typeMatcher = typePattern.matcher(splitSts[EvideoOutVIDFormatEnum.TYPE.ordinal()]);
		if(verMatcher.matches()){			
        	if (typeMatcher.matches()) {
        		vid = splitSts[EvideoOutVIDFormatEnum.VIDEOID.ordinal()];
        	}
        	else{
        		vid = splitSts[EvideoOutVIDFormatEnum.VIDEOID.ordinal()-1];
        	}
        }
        else{
        	vid = splitSts[EvideoOutVIDFormatEnum.VIDEOID.ordinal()-1];
        }
		return vid;		
	}


}
