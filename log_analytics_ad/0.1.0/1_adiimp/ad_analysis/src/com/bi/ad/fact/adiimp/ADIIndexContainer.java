package com.bi.ad.fact.adiimp;


public class ADIIndexContainer {
    
    public void setBrowse(int browse) {
        this.browse = browse;
    }

    public void setPlay(int play) {
        this.play = play;
    }

    public void setEffePlay(int effePlay) {
        this.effePlay = effePlay;
    }

    public void setFullPlay(int fullPlay) {
        this.fullPlay = fullPlay;
    }

    public void setClick(int click) {
        this.click = click;
    }


    public void add(String str) {
        
        String[] fields = str.split("\t");
        browse   += Integer.parseInt(fields[0]);
        play     += Integer.parseInt(fields[1]);
        effePlay += Integer.parseInt(fields[2]);
        fullPlay += Integer.parseInt(fields[3]);
        click    += Integer.parseInt(fields[4]);
    }

    public String toString() {
        StringBuilder str = new StringBuilder();
        String sep = "\t";
        str.append(String.valueOf(browse)).append(sep)
                .append(String.valueOf(play)).append(sep)
                .append(String.valueOf(effePlay)).append(sep)
                .append(String.valueOf(fullPlay)).append(sep)
                .append(String.valueOf(click));
        return str.toString();
    }

    public boolean isEmpty(){
        if( browse == 0 && play == 0 && effePlay == 0 && fullPlay == 0 && click == 0)
            return true;
         return false;
    }
    private int browse = 0;

    private int play = 0;

    private int effePlay = 0;

    private int fullPlay = 0;

    private int click = 0;

}
