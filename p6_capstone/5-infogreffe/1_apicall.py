import requests
import json

infogreffe_url = "https://opendata.datainfogreffe.fr/api/records/1.0/download/"
query = {
    "dataset": 'chiffres-cles-2019',
    "format":"json",
    "fields":"denomination,siren,nic,forme_juridique,code_ape,libelle_ape,adresse,code_postal,ville,date_de_cloture_exercice_1,duree_1,ca_1,resultat_1,effectif_1"
}
response = requests.get(infogreffe_url, params=query)
r = response.json()
with open('infogreffe.json', 'w') as outfile:
    json.dump(r, outfile)
