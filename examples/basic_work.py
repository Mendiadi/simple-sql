import simpleSQL
# Serverless (sqlite) example

# db model class
class OrderModel:
    # define columns in constructor
    def __init__(self,order_id, name, products, details):
        self.order_id = order_id
        self.name = name
        self.products = products
        self.details = details


# create db with serverless means use sqlite
def create_db():
    # to connect with mysql server  use  'with simpleSQL.connect(host=,user=,password=,database="orders.db") as db:'
    with simpleSQL.connect(serverless=True,database="orders.db") as db:
        db.commit()

def create_table():

    with simpleSQL.connect(serverless=True,database="orders.db") as db:
        model = OrderModel(
            db.types.column(db.types.integer(),nullable=False,auto_increment=True),
            db.types.column(db.types.varchar(50)),
            db.types.column(db.types.objType()),
            db.types.column(db.types.objType())
        )
        db.create_table(OrderModel,model,"order_id")
        db.commit()

# insert example (you use AUTO_INC if you set autoincrement on this attribute)
def insert(name,products,details):
    with simpleSQL.connect(serverless=True,database="orders.db") as db:
        model = OrderModel(
            db.AUTO_INC, name,products,details
        )
        db.insert_to(OrderModel,model)
        db.commit()

def update(model):
    with simpleSQL.connect(serverless=True, database="orders.db") as db:

        db.query_update_table(OrderModel,model)
        db.commit()

def get_all():
    with simpleSQL.connect(serverless=True,database="orders.db") as db:
        res = db.query_all(OrderModel)
        return res

def get_by_col(col,val):
    with simpleSQL.connect(serverless=True, database="orders.db") as db:
        return db.query_filter_by(OrderModel, col,val,first=True)

def delete(col,value):
    with simpleSQL.connect(serverless=True, database="orders.db") as db:
        db.query_delete_by(OrderModel,(col,value))
        db.commit()

def main():

    create_db()
    create_table()
    insert("myorder", ["banana", "apple", "milk"], {"amount": 3, "price": 100})
    insert("myorder1", None, {"amount": 0, "price": 0})
    insert("myorder2", ["meat", "apple", "bread"], {"amount": 3, "price": 150})
    insert("myorder3", ["kiwi", "cookies"], {"amount": 2, "price": 60})
    delete("name","myorder1")
    orders = get_all()
    for order in orders:
        print(order.order_id, order.name)
        if not order.products:
            continue
        for p in order.products:
            print(p)
        print(order.details['price'])
        print("**********************")
    order_to_update = orders[0]
    order_to_update.name,order_to_update.products,order_to_update.details  =\
        "new_name", ["water","bloons"], {"amount": 2, "price": 1}
    print("updated: ")
    update(order_to_update)
    order = get_by_col("name","new_name")
    print(order.name,order.order_id,order.products,order.details)


if __name__ == '__main__':
    main()