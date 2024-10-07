from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, EmailStr
from bson import ObjectId, errors as bson_errors
from pymongo import MongoClient
from typing import Optional, Dict, List
from datetime import datetime
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

