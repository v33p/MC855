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

def map_xml (xml_file):
    p = re.compile ('\\[.+?\\]')

    sc = SparkContext (appName="PythonPageParse")

    lines = sc.textFile (xml_file, 1)
    

def parseXml (xml_file):
    # pattern para para encontrar proximos links
    p = re.compile ('\\[.+?\\[')
    # iniciando arvore xml
    tree = ET.parse (xml_file)
    # adiquirindo root
    root = tree.getroot ()
    
    # para cada pagina
    for page in root.iter ('page'):
        if (validPage (page)):
            if (isPerson (page)):
                links = p.findall (page.find ('text'))
                for link in links:
                    
                #matcher = p.match (page.find ('text'))
                
