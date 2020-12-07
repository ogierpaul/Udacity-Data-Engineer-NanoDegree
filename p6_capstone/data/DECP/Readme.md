# Données essentielles de la commande publique - fichiers consolidés
## Origine
- [URL](https://www.data.gouv.fr/fr/datasets/r/16962018-5c31-4296-9454-5998585496d2)
- [Description](https://www.data.gouv.fr/fr/datasets/5cd57bf68b4c4179299eb0e9/)
- [GitHub](https://github.com/139bercy/decp-rama)


## Transformation
- Steps:
- Create the tables (3 tables, see details below)
- Transform to a JSON Lines format the raw json (Format_json_jq.sh)
- Copy to a staging_table decp_json
- Extract the information from the staging_table decp_json to:
    - a table with the attributes of each purchase (decp_attributes)
    - a table with the suppliers of each purchase (decp_titulaires)

