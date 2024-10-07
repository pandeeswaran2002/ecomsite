from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, EmailStr
from bson import ObjectId, errors as bson_errors
from pymongo import MongoClient
from typing import Optional, Dict, List
from datetime import datetime, timedelta
import logging



app = FastAPI()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)





client = MongoClient("mongodb://localhost:27017/")
db = client.ecom_platform
users_collection = db.users
orders_collection = db.orders
products_collection = db.products

class ObjectIdModel(BaseModel):
    __root__: str

    class Config:
        arbitrary_types_allowed = True

class Address(BaseModel):
    street: Optional[str]
    city: Optional[str]
    zipcode: Optional[str]
    country: Optional[str]

class products(BaseModel):
    product_id: str
    quantity: int
    price_per_unit: int



class user_data(BaseModel):
    name: str
    email: EmailStr
    age: int
    address: Address
    is_premium_member: bool
    date_joined: datetime
    referral_code: Optional[str]
    referred_by: Optional[str]

class orders_data(BaseModel):
    user_id: ObjectIdModel
    order_date: datetime
    total_amount: int
    products: products
    status: str

class product_data(BaseModel):
    name: str
    category: str
    price: int  
    stock: int
    rating: int  
    tags: List[str]  
    discount: int  
    last_updated: datetime

def user_to_dict(user):
    user_dict = user.copy()
    user_dict["id"] = str(user_dict["_id"])
    del user_dict["_id"]
    return user_dict

def order_to_dict(order):
    order_dict = order.copy()
    order_dict["id"] = str(order_dict["_id"])
    order_dict["user_id"] = str(order_dict["user_id"])
    del order_dict["_id"]
    return order_dict
def product_to_dict(product):
    product_dict = product.copy()
    product_dict["id"] = str(product_dict["_id"])
    del product_dict["_id"]
    return product_dict

@app.post("/user/createuserdata/", response_model=user_data)
async def create_user(user: user_data):
    try:
        user_dict = user.dict()
        user_dict["_id"] = ObjectId()
        users_collection.insert_one(user_dict)
        logger.info(f"user created with ID: {user_dict['_id']}")
        return user_to_dict(user_dict)
    except Exception as e:
        logger.error(f"Error creating job: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
@app.post("/order/createorder/", response_model=orders_data)
async def create_order(order: orders_data):
    try:
        order_dict = order.dict()
        order_dict["user_id"] = ObjectId(order.user_id.__root__)  
        order_dict["_id"] = ObjectId()
        orders_collection.insert_one(order_dict)
        logger.info(f"orders created with ID: {order_dict['_id']}")
        return order_to_dict(order_dict)
    except bson_errors.InvalidId:
        logger.error(f"Invalid Job ID format in application: {order.user_id.__root__}")
        raise HTTPException(status_code=400, detail="Invalid user ID format")
    except Exception as e:
        logger.error(f"Error creating application: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
@app.post("/product/createproduct/", response_model=product_data)
async def create_product(product: product_data):
    try:
        product_dict = product.dict()
        product_dict["_id"] = ObjectId()
        products_collection.insert_one(product_dict)
        logger.info(f"product created with ID: {product_dict['_id']}")
        return product_to_dict(product_dict)
    except Exception as e:
        logger.error(f"Error creating product: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/top-products/", response_model=List[Dict])
async def top_products():
    six_months_ago = datetime.now() - timedelta(days=180)
    pipeline = [
        {
            "$match": {
                "order_date": {"$gte": six_months_ago}
            }
        },
        {
            "$unwind": "$products"
        },
        {
            "$group": {
                "_id": "$products.product_id",
                "total_units_sold": {"$sum": "$products.quantity"},
                "total_revenue": {"$sum": {"$multiply": ["$products.quantity", "$products.price_per_unit"]}}
            }
        },
        {
            "$lookup": {
                "from": "products",
                "localField": "_id",
                "foreignField": "_id",
                "as": "product_info"
            }
        },
        {
            "$unwind": "$product_info"
        },
        {
            "$project": {
                "product_name": "$product_info.name",
                "category": "$product_info.category",
                "total_units_sold": 1,
                "total_revenue": 1
            }
        },
        {
            "$sort": {
                "total_units_sold": -1
            }
        },
        {
            "$limit": 5
        }
    ]

    top_products = list(orders_collection.aggregate(pipeline))
    return top_products

# Question 2: High-Value Users
@app.get("/high-value-users/", response_model=List[Dict])
async def high_value_users():
    pipeline = [
        {
            "$lookup": {
                "from": "orders",
                "localField": "_id",
                "foreignField": "user_id",
                "as": "user_orders"
            }
        },
        {
            "$unwind": {
                "path": "$user_orders",
                "preserveNullAndEmptyArrays": True
            }
        },
        {
            "$group": {
                "_id": "$_id",
                "name": {"$first": "$name"},
                "email": {"$first": "$email"},
                "total_spent": {"$sum": "$user_orders.total_amount"}
            }
        },
        {
            "$match": {
                "total_spent": {"$gt": 10000}
            }
        },
        {
            "$project": {
                "name": 1,
                "email": 1,
                "total_spent": 1
            }
        }
    ]

    high_value_users = list(users_collection.aggregate(pipeline))
    return high_value_users

# Question 3: Customer Referral Chain
@app.get("/referral-chain/{user_id}", response_model=List[Dict])
async def referral_chain(user_id: str):
    user_object_id = ObjectId(user_id)
    pipeline = [
        {
            "$match": {
                "_id": user_object_id
            }
        },
        {
            "$graphLookup": {
                "from": "users",
                "startWith": "$referral_code",
                "connectFromField": "referred_by",
                "connectToField": "referral_code",
                "as": "referral_chain",
                "depthField": "level"
            }
        },
        {
            "$project": {
                "name": 1,
                "referral_chain": 1
            }
        }
    ]

    referral_chain = list(users_collection.aggregate(pipeline))
    return referral_chain

# Question 4: Premium Member Retention
@app.get("/premium-retention/", response_model=Dict)
async def premium_retention():
    three_months_ago = datetime.now() - timedelta(days=90)
    one_year_ago = datetime.now() - timedelta(days=365)

    pipeline = [
        {
            "$match": {
                "is_premium_member": True,
                "date_joined": {"$lt": one_year_ago}
            }
        },
        {
            "$lookup": {
                "from": "orders",
                "localField": "_id",
                "foreignField": "user_id",
                "as": "orders"
            }
        },
        {
            "$match": {
                "orders.order_date": {"$gte": three_months_ago}
            }
        },
        {
            "$group": {
                "_id": None,
                "total_premium_members": {"$sum": 1}
            }
        }
    ]

    result = list(users_collection.aggregate(pipeline))
    total_premium_members = result[0]["total_premium_members"] if result else 0

    # Assuming you have a way to get total premium member count
    total_premium_member_count = await users_collection.count_documents({"is_premium_member": True})

    return {
        "percentage": (total_premium_members / total_premium_member_count * 100) if total_premium_member_count > 0 else 0,
        "count": total_premium_members
    }

# Question 5: Stock Prediction Query
@app.get("/projected-stock/", response_model=List[Dict])
async def projected_stock():
    three_months_ago = datetime.now() - timedelta(days=90)

    pipeline = [
        {
            "$lookup": {
                "from": "orders",
                "localField": "_id",
                "foreignField": "products.product_id",
                "as": "product_orders"
            }
        },
        {
            "$unwind": {
                "path": "$product_orders",
                "preserveNullAndEmptyArrays": True
            }
        },
        {
            "$match": {
                "product_orders.order_date": {"$gte": three_months_ago}
            }
        },
        {
            "$group": {
                "_id": "$_id",
                "name": {"$first": "$name"},
                "current_stock": {"$first": "$stock"},
                "average_sales_per_day": {"$avg": "$product_orders.quantity"}
            }
        },
        {
            "$project": {
                "projected_stock": {
                    "$subtract": ["$current_stock", {"$multiply": ["$average_sales_per_day", 30]}]  # 30 days for a month
                },
                "name": 1,
                "current_stock": 1
            }
        }
    ]

    projected_stock = list(products_collection.aggregate(pipeline))
    return projected_stock

# Question 6: Cancelled Orders Analysis
@app.get("/cancelled-orders/", response_model=List[Dict])
async def cancelled_orders():
    one_year_ago = datetime.now() - timedelta(days=365)

    pipeline = [
        {
            "$match": {
                "status": "canceled",
                "order_date": {"$gte": one_year_ago}
            }
        },
        {
            "$group": {
                "_id": "$user_id",
                "canceled_count": {"$sum": 1}
            }
        },
        {
            "$match": {
                "canceled_count": {"$gt": 2}
            }
        },
        {
            "$lookup": {
                "from": "users",
                "localField": "_id",
                "foreignField": "_id",
                "as": "user_info"
            }
        },
        {
            "$unwind": "$user_info"
        },
        {
            "$project": {
                "name": "$user_info.name",
                "email": "$user_info.email",
                "canceled_count": 1
            }
        }
    ]

    canceled_orders = list(orders_collection.aggregate(pipeline))
    return canceled_orders
