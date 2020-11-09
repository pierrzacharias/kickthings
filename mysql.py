# -*- coding: utf-8 -*
#  #############################################################################
#    Script for writing the SQL database for page_jaune
#  #############################################################################

import mysql.connector

mycursor = mydb.cursor()
mycursor.execute('''DROP table entreprise_pb;''')
mydb.commit()

mycursor.execute('''CREATE TABLE entreprise_pb (
  `id` varchar(150) NOT NULL,
  `nom_de_societe` varchar(255) DEFAULT NULL COMMENT 'Nom de societe',
  `adresse` varchar(255) DEFAULT NULL,
  `code_postal` decimal(5,0) DEFAULT NULL,
  `ville` varchar(255) DEFAULT NULL,
  `secteur_activite` varchar(500) DEFAULT NULL,
  `description` varchar(1000) DEFAULT NULL,
  `sites_reseaux_sociaux` varchar(255) DEFAULT NULL,
  `tel` varchar(255) DEFAULT NULL,
  `mobile` varchar(255) DEFAULT NULL,
  `fax` varchar(255) DEFAULT NULL,
  `siret` bigint(20) DEFAULT NULL,
  `code_naf` varchar(5) DEFAULT NULL,
  `effectif_etablissement` varchar(45) DEFAULT NULL,
  `typologie_tablissement` varchar(45) DEFAULT NULL,
  `siren` varchar(45) DEFAULT NULL,
  `siege_entreprise` varchar(255) DEFAULT NULL,
  `forme_juridique` varchar(255) DEFAULT NULL,
  `date_creation` varchar(45) DEFAULT NULL,
  `capital_social` varchar(45) DEFAULT NULL,
  `effectif_entreprise` varchar(45) DEFAULT NULL,
  `tva_intracommunautaire` varchar(45) DEFAULT NULL,
  `principaux_dirigeants` varchar(255) DEFAULT NULL,
  `autres_denominations` varchar(500) DEFAULT NULL,
  `chiffre_affaires` varchar(45) DEFAULT NULL,
  `excedent_brut_exploitation` varchar(45) DEFAULT NULL,
  `lat` double DEFAULT NULL,
  `lng` double DEFAULT NULL,
  `position` point NOT NULL,
  PRIMARY KEY (`id`),
  SPATIAL KEY `geo` (`position`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE utf8_general_ci;''')
mydb.commit()
# make the database
for line in open('all_data_Ile_de_France-0-65630.sql', encoding='utf8'):
    mycursor.execute(line)
    mydb.commit()
for line in open('all_data_Ile_de_France-65630-1500000.sql', encoding='utf8'):
    mycursor.execute(line)
    mydb.commit()
# ##################################################################################################

# ################################################
# Convert csv to .sql file and inject data into
# mySQL database
# ###############################################

from datetime import datetime
import logging
import json
import mysql.connector
from decimal import *
import pprint
import os
def connect_mySQL():
    mydb = mysql.connector.connect(
        host = "host",
        user = "pierre",
        passwd = "passwd",
        charset = "utf8",
        db = "pierre"
    )
    mycursor = mydb.cursor()
    print('Yay Connect to mySQL')
    return mydb, mycursor

# pour creer la table
# source C:/Users/pierr/Documents/Octopeek/donnees_entreprises/matching/entreprise_table.sql;
# with open('DATA_page_jaune/entreprise_table') as create_table:

def create_table(mycursor,mydb,table_name):
    mycursor.execute("DROP table IF EXISTS %s;" %(table_name))
    mydb.commit()
    create_table_request = "CREATE table IF NOT EXISTS entreprise_pb ( employe_id int NOT NULL AUTO_INCREMENT,id varchar(150) NOT NULL, nom_de_societe varchar(255) DEFAULT NULL COMMENT 'Nom de societe',adresse varchar(255) DEFAULT NULL,code_postal decimal(5,0) DEFAULT NULL,ville varchar(255) DEFAULT NULL,secteur_activite varchar(500) DEFAULT NULL,description varchar(1000) DEFAULT NULL,sites_reseaux_sociaux varchar(255) DEFAULT NULL,tel varchar(255) DEFAULT NULL,mobile varchar(255) DEFAULT NULL,fax varchar(255) DEFAULT NULL,siret bigint(20) DEFAULT NULL,code_naf varchar(5) DEFAULT NULL,effectif_etablissement varchar(45) DEFAULT NULL,typologie_etablissement varchar(45) DEFAULT NULL,siren varchar(45) DEFAULT NULL,siege_entreprise varchar(255) DEFAULT NULL,forme_juridique varchar(255) DEFAULT NULL,date_creation varchar(45) DEFAULT NULL,capital_social varchar(45) DEFAULT NULL,effectif_entreprise varchar(45) DEFAULT NULL,tva_intracommunautaire varchar(45) DEFAULT NULL,principaux_dirigeants varchar(255) DEFAULT NULL,autres_denominations varchar(500) DEFAULT NULL,chiffre_affaires varchar(45) DEFAULT NULL,excedent_brut_exploitation varchar(45) DEFAULT NULL,lat double DEFAULT NULL,lng double DEFAULT NULL,position point NOT NULL,PRIMARY KEY (employe_id), SPATIAL KEY geo (position)) ENGINE=MyISAM DEFAULT CHARSET=utf8 AUTO_INCREMENT=134 ;"
    mycursor.execute(create_table_request)
    mydb.commit()
    return None

def insert(mycursor,mydb,file,outfile):
    file_path = os.path.join('insert',file+'.sql')

    nombre_ligne_inseree = 0
    nombre_ligne_non_inseree = 0

    for line in open(file_path, encoding='utf8'):
        try :
            mycursor.execute(line)
            mydb.commit()
            nombre_ligne_inseree +=1
        except:
            nombre_ligne_non_inseree +=1
            pass


    print("nombre d'entreprise inseree pour "+file+" : %s" %(nombre_ligne_inseree))
    print("nombre d'entreprise inseree pour "+file+" : %s" %(nombre_ligne_non_inseree))
    outfile.write("nombre d'entreprise inseree pour "+file+" : %s \n" %(nombre_ligne_inseree))
    outfile.write("nombre d'entreprise inseree pour "+file+" : %s \n" %(nombre_ligne_non_inseree))
    return None

list_file = [ ]
mydb, mycursor = connect_mySQL()
create_table(mycursor,mydb,'entreprise_pb')
outfile = open('nombre_ligne_inseree_2.txt', 'w')
for file in list_file:
    print("insertion table %s" %file)
    insert(mycursor,mydb,file,outfile)
outfile.close()
mydb.close()

def compter_doublon(es):
    es = connect_elasticsearch()
    mydb,mycursor = connect_mySQL()


    mycursor.execute('''SELECT COUNT(*) FROM entreprise_pb  ORDER BY id''')
    nombre_ligne_result = mycursor.fetchall()
    nombre_ligne = nombre_ligne_result[0][0]
    print(nombre_ligne)


    pas = 100
    ligne_page_jaune = 0

    while ligne_page_jaune < nombre_ligne:
        mycursor.execute("SELECT * FROM entreprise_pb  ORDER BY id LIMIT %s OFFSET %s " % (pas,ligne_page_jaune))
        resultat = mycursor.fetchall()
        #print(resultat)
        #print(len(resultat))
        for i in range(len(resultat)):

            #print(resultat[i][1])
            search_object = {
                "query": {
                      "match_phrase": {
                          "company_name" : resultat[i][1]
                        }
                    }
            }
            res = search(es, search_object, index_name = 'page_jaune_company_name')
            if res["hits"]["total"] > 1:

                print("doublon :",resultat[i][1],res["hits"]["total"] )
                #pprint.pprint(res)

        ligne_page_jaune += pas
    mydb.close()
# ##################################################################################################

# -*- coding: utf-8 -*-
"""
Création et remplissage d'une table SQL
"""
def insert_tuple(es_data, list_field,afficher=False):
    insert_list = [''] * len(list_field)
    for i in range(len(list_field)):
        try:
            insert_list[i] = es_data[list_field[i]]
            if afficher:
                print(list_field[i],es_data[list_field[i]])
        except Exception:
            pass
    if afficher:
        print(tuple(insert_list))
        print("\n")
    return tuple(insert_list)

def print_insert(insert):
    for i in range(len(insert)):
        print(i,insert[i])
    print('\n')
    return None


if __name__ == '__main__':

    count_profile_inserted = 0
    # connect to databases
    mydb,mycursor = connect_mySQL()
    mycursor.execute('''DROP table IF EXISTS entreprise_test;''')
    mycursor.execute('''CREATE TABLE entreprise_test (
    id varchar(150) NOT NULL,
    lat double DEFAULT NULL,
    lng double DEFAULT NULL,
    position point NOT NULL,
    nombre_employees int DEFAULT NULL,
    PRIMARY KEY (id),
    SPATIAL KEY geo (`position`)
    ) ENGINE=MyISAM DEFAULT CHARSET=utf8;
    ''')
    mydb.commit()

    listPoint = str([ (48, 3), (48, 3), (49, 3), (49, 3), (48, 3) ])

    #mycursor.execute("SELECT id,lat,lng,position FROM entreprise_pb WHERE MBRWithin(position, ST_GeomFromText('Polygon(("+listPoint+"))')) LIMIT 10;")
    mycursor.execute("SELECT id,lat,lng,position  FROM entreprise_pb LIMIT 100000;")
    resultat = mycursor.fetchall()

    insert_list = []
    for res in resultat:
        insert = (res[0], res[1], res[2], res[3], random.randint(0,500))
        insert_list.append( insert )



    #pprint.pprint(insert_list)
    sql_insert =  '''INSERT INTO entreprise_test (id,lat,lng,position, nombre_employees)
                VALUES (%s, %s, %s, %s, %s)
                '''
    #print_insert(insert_list)
    mycursor.executemany(sql_insert,insert_list)
    mydb.commit()

    print("base de test",mycursor.rowcount, "record inserted.")
    count_profile_inserted += mycursor.rowcount
    print("nombre de profil inséré dans la base sql : ",count_profile_inserted)
    mydb.close()

# ##################################################################################################
def create_table(mycursor,mydb,table_name):
    mycursor.execute("DROP table IF EXISTS %s;" %(table_name))
    mydb.commit()
    with open("%s.sql" %(table_name)) as table_employees:
        table_employees_file = table_employees.read()
        mycursor.execute(table_employees_file)
    mydb.commit()
    return None
create_table(mycursor,mydb,"employees")
if __name__ == '__main__':

    # connect to databases
    mydb,mycursor = connect_mySQL()
    es = connect_elasticsearch()

    create_table(mycursor,mydb,"employees")
    while profile_i < total_number_of_profile:

        search_object = {
            "from" : profile_i, "size" : step,
            "query": {
                "match_all": {}
            }
        }
        res = search(es,search_object,index_name = 'company_employees_url_keywords')
        for hit in res['hits']['hits']:
            sql_employees_insert =  '''INSERT INTO employees (company_id,id,score_affiliation,canonical_shorthand_name,canonical_shorthand_name_hash,canonical_url, connections,connections_count,country,created,deleted,experience_count, experiences_by_months, hash,  industry,  last_response_code,  last_updated,  last_updated_ux, location,  logo_url,  member_shorthand_name, member_shorthand_name_hash, name,  outdated,  recommendations_count, summary, title, url
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s,  %s, %s,  %s,  %s,  %s,  %s, %s, %s,  %s,  %s,  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                '''
            sql_member_websites_insert =  '''INSERT INTO employees (created, deleted, id, last_updated, member_id, website)
                VALUES (%s, %s, %s, %s, %s, %s)
                '''
            list_employees_insert = []
            list_member_websites_insert = []
            for employee in hit["_source"]["employees"]:
                list_field = ["id","score_affiliation","canonical_shorthand_name","canonical_shorthand_name_hash","canonical_url","connections","connections_count","country","created","deleted","experience_count"," experiences_by_months","hash","industry","last_response_code","last_updated","last_updated_ux","location","logo_url","member_shorthand_name","member_shorthand_name_hash","name","outdated","recommendations_count","summary","title","url"]
                insert_employees_tuple = insert_tuple(employee,list_field)
                insert_employees_tuple = tuple([company_id] + list(insert_employees_tuple))
                list_employees_insert.append( insert_employees_tuple )


                #for member_websites in employee["member_websites"]:
                    #list_field = ["created","deleted","id","last_updated","member_id","website"]
                    #insert_member_websites_tuple = insert_tuple(member_websites,list_field)
                    #list_member_websites_insert.append(insert_member_websites_tuple)

            # #############################################################################
            # inserstion des donnees par paquet pour chaque entreprise
            # ############################################################################
            mycursor.executemany(sql_employees_insert,list_employees_insert)
            mydb.commit()
            print("employees",mycursor.rowcount, "record inserted.")
            mycursor.executemany(sql_member_websites_insert,     list_member_websites_insert)
        profile_i += step


# -*- coding: utf-8 -*-
# ##################################################################
# insert data from elasticsearch into mysql
# ##################################################################
def insert_tuple(es_data, list_field,afficher=False):
    insert_list = [''] * len(list_field)
    for i in range(len(list_field)):
        try:
            insert_list[i] = es_data[list_field[i]]
            if afficher:
                print(list_field[i],es_data[list_field[i]])
        except Exception:
            pass
    if afficher:
        print(tuple(insert_list))
        print("\n")
    return tuple(insert_list)

if __name__ == '__main__':

    # #############################################################################
    # inserstion des donnees par paquet dans la base sql pour chaque entreprise
    # ############################################################################
    mydb,mycursor = connect_mySQL()
    es = connect_elasticsearch()
    create_table(mycursor,mydb,"employees_info")
    total_number_of_profile =  compter_ligne(es, 'company_employees_url_keywords')
    print(total_number_of_profile)
    count_profile_inserted = 0
    step = 100
    profile_i = 0
    while profile_i < total_number_of_profile:
        search_object = {
            "from" : profile_i, "size" : step,
            "query": {
                "match_all": {}
            }
        }
        res = search(es,search_object,index_name = 'company_employees_url_keywords')
        for hit in res['hits']['hits']:
            sql_employees_info_insert =  '''INSERT INTO employees_info (company_id,id,score_affiliation,name,title,url)
                VALUES (%s, %s, %s, %s, %s, %s)
                '''
            list_employees_info_insert = []
            company_id = hit["_source"]["company_id"]
            for employee in hit["_source"]["employees"]:
                list_field = ["id","score_affiliation","name","title","url"]
                insert_employees_info_tuple = insert_tuple(employee,list_field)
                insert_employees_info_tuple = tuple([company_id] + list(insert_employees_info_tuple))
                #print(len(insert_employees_tuple))
                #print_insert(insert_employees_tuple)
                list_employees_info_insert.append( insert_employees_info_tuple )
            mycursor.executemany(sql_employees_info_insert,list_employees_info_insert)
            mydb.commit()
            print("employees",mycursor.rowcount, "record inserted.")
            count_profile_inserted += mycursor.rowcount
        profile_i += step
    with open('count_profile_inserted.txt', 'w') as outfile_3:
        print("nombre de profil inséré dans la base sql : ",count_profile_inserted)
        outfile_3.write(str(count_profile_inserted))
        outfile_3.close()
    mydb.close()
# ##################################################################################################
