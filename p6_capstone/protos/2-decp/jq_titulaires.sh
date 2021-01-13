{
"decp_uid": .uid, "titulaires": .titulaires[]?
}  | {
  "decp_uid": .decp_uid,
"titulaire_id": .titulaires.id,
"titulaire_name":.titulaires.denominationSociale,
"titulaire_typeidentifiant":.titulaires.typeIdentifiant
}