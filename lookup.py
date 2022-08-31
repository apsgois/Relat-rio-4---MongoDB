from db.database import Database
from helper.WriteAJson import writeAJson
from dataset.pessoa_dataset import dataset as pessoa_dataset
from dataset.carro_dataset import dataset as carro_dataset
from dataset.produto_database import dataset as produto_dataset

pessoas = Database(
    database="database",
    collection="pessoas",
    dataset=pessoa_dataset
)
pessoas.resetDatabase()

carros = Database(
    database="database",
    collection="carros",
    dataset=carro_dataset
)
carros.resetDatabase()

produtos = Database(
    database="database",
    collection="produtos",
    dataset=produto_dataset
)
produtos.resetDatabase()
result1 = carros.collection.aggregate([
    {"$lookup":
        {
            "from": "pessoas",  # outra colecao
            "localField": "dono_id",  # chave estrangeira
            "foreignField": "_id",  # id da outra colecao
            "as": "dono"  # nome da saida
        }
     }
])

result2 = produtos.collection.aggregate([
    {"$lookup":
        {
            "from": "pessoas",  # outra colecao
            "localField": "cliente_id",  # chave estrangeira
            "foreignField": "_id",  # id da outra colecao
            "as": "compras"  # nome da saida
        }
     },

    #{"$group": {"_id": "$cliente_id", "total": {"$sum": "$total", "nome":"pessoa.nome" }}}, # formata os documentos
    {"$sort": {"total": -1}},
    {"$unwind": "$compras"},
    {"$project": {
        "_id": 0,
        "total": 1,
        "nome": "$compras.nome",
        "desconto": {
            "$cond": {"if": {"$gte": ["$total", 10]}, "then": "True", "else": "False"}
        }
    }
    }

])
writeAJson(result2, "result2")

