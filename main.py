from db.database import Database
from helper.WriteAJson import writeAJson
from dataset.produto_database import dataset

compras = Database(database="database", collection="produtos", dataset=dataset)
#compras.resetDatabase()

result1 = compras.collection.aggregate([
    {"$group": {"_id": "$cliente_id", "total": {"$sum": "$total"} } }, # formata os documentos
    {"$sort": {"total": -1} },
    {"$project": {
        "_id": 0,
        "total": 1,
        "maior_que": {
            "$cond": {"if": {"$gte": ["$total", 30]}, "then": "sim", "else": "nao"}
        }
    }}
])

writeAJson(result1, "result1")


