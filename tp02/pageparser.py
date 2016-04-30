import xml.etree.ElementTree

from pyspark import SparkContext

import re
import sys

pattern = ["Político", "Cientista", "Lutador", "Papa", "Ator", "Futebolista", "Escritor", "Biografia", "esporte/atleta", "Esporte/Atleta", "piloto", "Piloto", "Autor", "Motorista", "Médico", "Jornalista", "Surfista", "Gamer", "Sacerdote", "Exadrista", "Lutador", "Beisebolista", "Treinador", "Astronauta", "Tenista", "Automobilista", "Filósofo", "Árbitro", "Arquiteto", "Jogador", "Boxeador", "Psicólogo", "Voleibol/Jogador", "Criminoso", "Patinador", "Empresario", "Serial Killer", "Nadador", "Arqueiro", "Revolucionário", "Fisiculturista"]

def 
