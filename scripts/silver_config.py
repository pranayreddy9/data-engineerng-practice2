silver_config = {

    "customers": {
        "pk": ["customer_id"],
        "cast": {
            "customer_id": "int"
        },
        "trim": ["first_name", "last_name", "email", "country"],
        "lower": ["email", "country"],
        "timestamp": [],
    },

    "products": {
        "pk": ["product_id"],
        "cast": {
            "product_id": "int",
            "price": "double"
        },
        "trim": ["product_name", "category"],
        "lower": ["category"],
        "timestamp": [],
    },

    "orders": {
        "pk": ["order_id"],
        "cast": {
            "order_id": "int",
            "customer_id": "int"
        },
        "trim": [],
        "lower": [],
        "timestamp": ["order_date"]
    },

    "order_items": {
        "pk": ["order_id", "product_id"],
        "cast": {
            "order_id": "int",
            "product_id": "int",
            "quantity": "int"
        },
        "trim": [],
        "lower": [],
        "timestamp": []
    },

    "payments": {
        "pk": ["payment_id"],
        "cast": {
            "payment_id": "int",
            "order_id": "int",
            "amount": "double"
        },
        "trim": ["method"],
        "lower": ["method"],
        "timestamp": []
    },

    "shipments": {
        "pk": ["shipment_id"],
        "cast": {
            "shipment_id": "int",
            "order_id": "int"
        },
        "trim": ["status"],
        "lower": ["status"],
        "timestamp": []
    },

    "returns": {
        "pk": ["return_id"],
        "cast": {
            "return_id": "int",
            "order_id": "int"
        },
        "trim": ["reason"],
        "lower": ["reason"],
        "timestamp": []
    }
}
