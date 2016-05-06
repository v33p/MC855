# -*- coding:UTF-8 -*- #

import xml.etree.ElementTree as ET

import re
import sys

patterns = ["Político", "Cientista", "Lutador", "Papa", "Ator", "Futebolista", "Escritor", "Biografia", "esporte/atleta", "Esporte/Atleta", "piloto", "Piloto", "Autor", "Motorista", "Médico", "Jornalista", "Surfista", "Gamer", "Sacerdote", "Exadrista", "Lutador", "Beisebolista", "Treinador", "Astronauta", "Tenista", "Automobilista", "Filósofo", "Árbitro", "Arquiteto", "Jogador", "Boxeador", "Psicólogo", "Voleibol/Jogador", "Criminoso", "Patinador", "Empresario", "Serial Killer", "Nadador", "Arqueiro", "Revolucionário", "Fisiculturista"]

def validPage (page):
    title = page.find ('title').text
    return (":" not in title)

def isPerson (page):
    text = page.find ('text').text
    result = False
    for p in patterns:
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
    print ('parse ' + str(xml_file))
    # para cada pagina
    for page in root.findall ('page'):
    #for page in root.iter ('page'):
        print (page.find ('title').text)
        if (validPage (page)):
            print (page.find ('title').text)
            if (isPerson (page)):
                print (page.find ('title').text)
                title = page.find ('title').text
                matcher = p.match (page.find ('text').text)
                links = p.findall (page.find ('text').text)
                for link in links:
                    print (link)
                    if (isWikiLink (link) == True):
                        link = getWikiPageFromLink (link)
                        print (link)
                        f.write (title + ' ' + link)
    f.close ()

if len (sys.argv) != 3:
    print "wrong parameters"
    exit (-1)
parseXml (sys.argv[1], sys.argv[2])
