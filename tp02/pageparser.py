!#/bin/bash/

import xml.etree.ElementTree

from pyspark import SparkContext

import re
import sys

patterns = ["Político", "Cientista", "Lutador", "Papa", "Ator", "Futebolista", "Escritor", "Biografia", "esporte/atleta", "Esporte/Atleta", "piloto", "Piloto", "Autor", "Motorista", "Médico", "Jornalista", "Surfista", "Gamer", "Sacerdote", "Exadrista", "Lutador", "Beisebolista", "Treinador", "Astronauta", "Tenista", "Automobilista", "Filósofo", "Árbitro", "Arquiteto", "Jogador", "Boxeador", "Psicólogo", "Voleibol/Jogador", "Criminoso", "Patinador", "Empresario", "Serial Killer", "Nadador", "Arqueiro", "Revolucionário", "Fisiculturista"]

def validPage (page):
    title = page.find ('title')
    return (":" not in title)

def isPerson (page):
    text = page.find ('text')
    result = False
    for p in patterns:
        if ("Info/"+p in text):
            result = True
    return result

def isWikiLink (link):
    start = 1
    if (link.startswith ("[["):
        start = 2
    if (len (link) < start+2 || len (link) > 100):
        return False
    if (link[start] == '#' ||
        link[start] == '.' ||
        #link[start] == '\' ||
        link[start] == '-' ||
        link[start] == '{'):
        return False
    if (":" in link ||
        "," in link ||
        "&" in link):
        return False
    return True

def getWikiPageFromLink (link):
    start = 1
    if (link.startswith("[["):
        start = 2
    end = link.index ("]")
    if ("|" in link):
        end = link.index ("|")
    if ("#" in link):
        end = link.index ("#")

    link = link[start:end]
    #link = link.replace ("\\s", "_")
    link = link.replace (",", "")
    link = link.replace ("&amp;", "&")

    return link

def parseXml (xml_file, parse_file):
    # pattern para para encontrar proximos links
    p = re.compile ('\\[.+?\\[')
    # iniciando arvore xml
    tree = ET.parse (xml_file)
    # adiquirindo root
    root = tree.getroot ()
    
    f = open (parse_file, 'w')

    # para cada pagina
    for page in root.iter ('page'):
        if (validPage (page)):
            if (isPerson (page)):
                title = page.find ('title')
                matcher = p.match (page.find ('text'))
                links = p.findall (page.find ('text'))
                for link in links:
                    if (isWikiLink (link) == True):
                        link = getWikiPageFromLink (link)
                        f.write (title + ' ' + link)

if __name__ == "__main__":
    if len (sys.argv) != 3:
        print "wrong parameters"
        exit (-1)
    parseXml (sys.argv[1], sys.argv[2])
