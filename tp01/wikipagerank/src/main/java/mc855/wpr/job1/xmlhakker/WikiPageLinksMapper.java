package mc855.wpr.job1.xmlhakker;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class WikiPageLinksMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private static final Pattern wikiLinksPattern = Pattern.compile("\\[.+?\\]");
    private static final Pattern
	    rx01 = Pattern.compile("Info/Político"),
	    rx02 = Pattern.compile("Info/Cientista"),
	    rx03 = Pattern.compile("Info/Lutador"),
	    rx04 = Pattern.compile("Info/Papa"),
	    rx05 = Pattern.compile("Info/Ator"),
	    rx06 = Pattern.compile("Info/Futebolista"),
	    rx07 = Pattern.compile("Info/Escritor"),
	    rx08 = Pattern.compile("Info/Biografia"),
	    rx09 = Pattern.compile("Info/Música/artista"),
	    rx10 = Pattern.compile("Info/Música/Artista"),
	    rx11 = Pattern.compile("Info/esporte/atleta"),
	    rx12 = Pattern.compile("Info/Esporte/Atleta"),
	    rx13 = Pattern.compile("Info/piloto"),
	    rx14 = Pattern.compile("Info/Piloto"),
	    rx15 = Pattern.compile("Info/Autor"),
	    rx16 = Pattern.compile("Info/Motorista"),
	    rx17 = Pattern.compile("Info/Médico"),
	    rx18 = Pattern.compile("Info/Jornalista"),
	    rx19 = Pattern.compile("Info/Surfista"),
	    rx20 = Pattern.compile("Info/Gamer"),
	    rx21 = Pattern.compile("Info/Sacerdote"),
	    rx22 = Pattern.compile("Info/Enxadrista"),
	    rx23 = Pattern.compile("Info/Lutador"),
	    rx24 = Pattern.compile("Info/Beisebolista"),
	    rx25 = Pattern.compile("Info/Treinador"),
	    rx26 = Pattern.compile("Info/Astronauta"),
	    rx27 = Pattern.compile("Info/Artista"),
	    rx28 = Pattern.compile("Info/Tenista"),
	    rx29 = Pattern.compile("Info/Automobilista"),
	    rx30 = Pattern.compile("Info/Filósofo"),
	    rx31 = Pattern.compile("Info/Árbitro"),
	    rx32 = Pattern.compile("Info/Arquiteto"),
	    rx33 = Pattern.compile("Info/Jogador"),
	    rx34 = Pattern.compile("Info/Boxeador"),
	    rx35 = Pattern.compile("Info/Psicólogo"),
	    rx36 = Pattern.compile("Info/Voleibol/Jogador"),
	    rx37 = Pattern.compile("Info/Criminoso"),
	    rx38 = Pattern.compile("Info/Patinador"),
	    rx39 = Pattern.compile("Info/Empresario"),
	    rx40 = Pattern.compile("Info/Serial Killer"),
	    rx41 = Pattern.compile("Info/Nadador"),
	    rx42 = Pattern.compile("Info/Arqueiro"),
	    rx43 = Pattern.compile("Info/Revolucionário"),
	    rx44 = Pattern.compile("Info/Fisiculturista");

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        // Returns  String[0] = <title>[TITLE]</title>
        //          String[1] = <text>[CONTENT]</text>
        // !! without the <tags>.
        String[] titleAndText = parseTitleAndText(value);
        
        String pageString = titleAndText[0];
        if(notValidPage(pageString))
            return;
        
        Text page = new Text(pageString.replace(' ', '_'));

        Matcher matcher = wikiLinksPattern.matcher(titleAndText[1]);
        
        Matcher
        	m01 = rx01.matcher(titleAndText[1]),
        	m02 = rx02.matcher(titleAndText[1]),
        	m03 = rx03.matcher(titleAndText[1]),
        	m04 = rx04.matcher(titleAndText[1]),
        	m05 = rx05.matcher(titleAndText[1]),
        	m06 = rx06.matcher(titleAndText[1]),
        	m07 = rx07.matcher(titleAndText[1]),
        	m08 = rx08.matcher(titleAndText[1]),
        	m09 = rx09.matcher(titleAndText[1]),
        	m10 = rx10.matcher(titleAndText[1]),
        	m11 = rx11.matcher(titleAndText[1]),
        	m12 = rx12.matcher(titleAndText[1]),
        	m13 = rx13.matcher(titleAndText[1]),
        	m14 = rx14.matcher(titleAndText[1]),
        	m15 = rx15.matcher(titleAndText[1]),
        	m16 = rx16.matcher(titleAndText[1]),
        	m17 = rx17.matcher(titleAndText[1]),
        	m18 = rx18.matcher(titleAndText[1]),
        	m19 = rx19.matcher(titleAndText[1]),
        	m20 = rx20.matcher(titleAndText[1]),
        	m21 = rx21.matcher(titleAndText[1]),
        	m22 = rx22.matcher(titleAndText[1]),
        	m23 = rx23.matcher(titleAndText[1]),
        	m24 = rx24.matcher(titleAndText[1]),
        	m25 = rx25.matcher(titleAndText[1]),
        	m26 = rx26.matcher(titleAndText[1]),
        	m27 = rx27.matcher(titleAndText[1]),
        	m28 = rx28.matcher(titleAndText[1]),
        	m29 = rx29.matcher(titleAndText[1]),
        	m30 = rx30.matcher(titleAndText[1]),
        	m31 = rx31.matcher(titleAndText[1]),
        	m32 = rx32.matcher(titleAndText[1]),
        	m33 = rx33.matcher(titleAndText[1]),
        	m34 = rx34.matcher(titleAndText[1]),
        	m35 = rx35.matcher(titleAndText[1]),
        	m36 = rx36.matcher(titleAndText[1]),
        	m37 = rx37.matcher(titleAndText[1]),
        	m38 = rx38.matcher(titleAndText[1]),
        	m39 = rx39.matcher(titleAndText[1]),
        	m40 = rx40.matcher(titleAndText[1]),
        	m41 = rx41.matcher(titleAndText[1]),
        	m42 = rx42.matcher(titleAndText[1]),
        	m43 = rx43.matcher(titleAndText[1]),
        	m44 = rx44.matcher(titleAndText[1]);
        	
        	
        if (m01.find() || m02.find() || m03.find() || m04.find() || m05.find() || m06.find() || m07.find() || m08.find() || m09.find() || m10.find() || m11.find() || m12.find() || m13.find() || m14.find() || m15.find() || m16.find() || m17.find() || m18.find() || m19.find() || m20.find() || m21.find() || m22.find() || m23.find() || m24.find() || m25.find() || m26.find() || m27.find() || m28.find() || m29.find() || m30.find() || m31.find() || m32.find() || m33.find() || m34.find() || m35.find() || m36.find() || m37.find() || m38.find() || m39.find() || m40.find() || m41.find() || m42.find() || m43.find() || m44.find()) {
        
        }
        else {
        	return;
        }
        //Loop through the matched links in [CONTENT]
        while (matcher.find()) {
            String otherPage = matcher.group();
            //Filter only wiki pages.
            //- some have [[realPage|linkName]], some single [realPage]
            //- some link to files or external pages.
            //- some link to paragraphs into other pages.
            otherPage = getWikiPageFromLink(otherPage);
            if(otherPage == null || otherPage.isEmpty()) 
                continue;
            
            // add valid otherPages to the map.
            context.write(page, new Text(otherPage));
        }
    }
    
    private boolean notValidPage(String pageString) {
        return pageString.contains(":");
    }

    private String getWikiPageFromLink(String aLink){
        if(isNotWikiLink(aLink)) return null;
        
        int start = aLink.startsWith("[[") ? 2 : 1;
        int endLink = aLink.indexOf("]");

        int pipePosition = aLink.indexOf("|");
        if(pipePosition > 0){
            endLink = pipePosition;
        }
        
        int part = aLink.indexOf("#");
        if(part > 0){
            endLink = part;
        }
        
        aLink =  aLink.substring(start, endLink);
        aLink = aLink.replaceAll("\\s", "_");
        aLink = aLink.replaceAll(",", "");
        aLink = sweetify(aLink);
        
        return aLink;
    }
    
    private String sweetify(String aLinkText) {
        if(aLinkText.contains("&amp;"))
            return aLinkText.replace("&amp;", "&");

        return aLinkText;
    }

    private String[] parseTitleAndText(Text value) throws CharacterCodingException {
        String[] titleAndText = new String[2];
        
        int start = value.find("<title>");
        int end = value.find("</title>", start);
        start += 7; //add <title> length.
        
        titleAndText[0] = Text.decode(value.getBytes(), start, end-start);

        start = value.find("<text");
        start = value.find(">", start);
        end = value.find("</text>", start);
        start += 1;
        
        if(start == -1 || end == -1) {
            return new String[]{"",""};
        }
        
        titleAndText[1] = Text.decode(value.getBytes(), start, end-start);
        
        return titleAndText;
    }

    private boolean isNotWikiLink(String aLink) {
        int start = 1;
        if(aLink.startsWith("[[")){
            start = 2;
        }
        
        if( aLink.length() < start+2 || aLink.length() > 100) return true;
        char firstChar = aLink.charAt(start);
        
        if( firstChar == '#') return true;
        if( firstChar == ',') return true;
        if( firstChar == '.') return true;
        if( firstChar == '&') return true;
        if( firstChar == '\'') return true;
        if( firstChar == '-') return true;
        if( firstChar == '{') return true;
        
        if( aLink.contains(":")) return true; // Matches: external links and translations links
        if( aLink.contains(",")) return true; // Matches: external links and translations links
        if( aLink.contains("&")) return true;
        
        return false;
    }
}
