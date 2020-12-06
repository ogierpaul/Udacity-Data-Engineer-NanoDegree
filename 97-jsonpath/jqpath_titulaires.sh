{
  "marches": .marches[]
} | {
"id": .marches.id, "child": .marches.childs[]?
}  | {
"marche_id": .id,
"child_id":.child.id ,
"name":.child.name
}