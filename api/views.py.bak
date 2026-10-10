from django.http import JsonResponse
from cassandra.cluster import Cluster

cluster = Cluster(["127.0.0.1"])
session = cluster.connect("dbms_test")

def test_cassandra(request):
    rows = session.execute("SELECT * FROM users")

    data = []

    for row in rows:
        data.append({
            "user_id": str(row.user_id),
            "name": row.name,
            "email": row.email
        })

    return JsonResponse({
        "status": "success",
        "database": "Cassandra",
        "users": data
    })