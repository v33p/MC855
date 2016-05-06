# -*- coding:utf-8 -*- #

import xml.etree.ElementTree as ET

import re
import sys
import unicodedata

xml_hc = '{http://www.mediawiki.org/xml/export-0.10/}'

#oldpatterns = ["Político", "Cientista", "Lutador", "Papa", "Ator", "Futebolista", "Escritor", "Biografia", "esporte/atleta", "Esporte/Atleta", "piloto", "Piloto", "Autor", "Motorista", "Médico", "Jornalista", "Surfista", "Gamer", "Sacerdote", "Exadrista", "Lutador", "Beisebolista", "Treinador", "Astronauta", "Tenista", "Automobilista", "Filósofo", "Árbitro", "Arquiteto", "Jogador", "Boxeador", "Psicólogo", "Voleibol/Jogador", "Criminoso", "Patinador", "Empresario", "Serial Killer", "Nadador", "Arqueiro", "Revolucionário", "Fisiculturista"]

patterns = ["Politico", "Cientista", "Lutador", "Papa", "Ator", "Futebolista", "Escritor", "Biografia", "esporte/atleta", "Esporte/Atleta", "piloto", "Piloto", "Autor", "Motorista", "Medico", "Jornalista", "Surfista", "Gamer", "Sacerdote", "Exadrista", "Lutador", "Beisebolista", "Treinador", "Astronauta", "Tenista", "Automobilista", "Filosofo", "Arbitro", "Arquiteto", "Jogador", "Boxeador", "Psicologo", "Voleibol/Jogador", "Criminoso", "Patinador", "Empresario", "Serial Killer", "Nadador", "Arqueiro", "Revolucionario", "Fisiculturista"]

def validPage (page):
    title = page.find (xml_hc+'title').text
    if (type (title) == type (unicode)):
        title = unicodedata.normalize ('NFKD', text).encode ('ascii', 'ignore') 
    return (":" not in title)

def isPerson (page):
    texts = page.iter (xml_hc+'text')
    for t in texts:
	text = t.text
    result = False
    if (type (text) == type (unicode)):
        text = unicodedata.normalize ('NFKD', text).encode ('ascii', 'ignore')
    #print type (text)
    for p in patterns:
	#print type (p)
        if ("Info/"+p in text):
            result = True
    return result

def isWikiLink (link):
    start = 1
    if (link.startswith ("[[")):
        start = 2
    if (len (link) < start+2 or len (link) > 100):
        return False
    if (link[start] == '#' or
        link[start] == '.' or
        #link[start] == '\' or
        link[start] == '-' or
        link[start] == '{'):
        return False
    if (":" in link or
        "," in link or
        "&" in link):
        return False
    return True

def getWikiPageFromLink (link):
    start = 1
    if (link.startswith("[[")):
        start = 2
    end = link.index ("]")
    if ("|" in link):
        end = link.index ("|")
    if ("#" in link):
        end = link.index ("#")

    link = link[start:end]
    link = link.replace (" ", "_")
    link = link.replace (",", "")
    link = link.replace ("&amp;", "&")

    return link

def parseXml (xml_file, parse_file):
    # pattern para para encontrar proximos links
    p = re.compile ('\\[.+?\\]')
    # iniciando arvore xml
    with open (xml_file, 'r') as xml_file2:
        tree = ET.parse (xml_file2)
    # adiquirindo root
    root = tree.getroot ()

    f = open (parse_file, 'w')

    print ('parse ' + str(xml_file))
    
    # para cada pagina
    for page in root.iter (xml_hc+'page'):
	title = page.find (xml_hc+'title').text
	title = title.replace (" ", "_")
        if (type (title) == type (unicode)):
            title = unicodedata.normalize ('NFKD', text).encode ('ascii', 'ignore') 
        print ('page found: ' + title)
        if (validPage (page)):
            print (title + ' validPage')
            if (isPerson (page)):
                print (title + ' is a person')
                texts = page.iter (xml_hc+'text')
		for t in texts:
		    text = t.text
    		if (type (text) == type (unicode)):
                    text = unicodedata.normalize ('NFKD', text).encode('ascii', 'ignore')
                matcher = p.match (text)
                links = p.findall (text)
                for link in links:
                    if (type (link) == type (unicode)):
                        link = unicodedata.normalize ('NFKD', link).encode ('ascii', 'ignore') 
                    print ('link found: ' +link)
                    if (isWikiLink (link) == True):
                        link = getWikiPageFromLink (link)
                        print (link + ' added')
                        if (type (link) == type (unicode)):
                            link = unicodedata.normalize ('NFKD', link).encode ('ascii', 'ignore')
                        f.write (str(title) + ' ' + str(link)+'\n')
    f.close ()

if len (sys.argv) != 3:
    print "wrong parameters"
    exit (-1)
parseXml (sys.argv[1], sys.argv[2])
