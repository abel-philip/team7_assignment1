# importing boto3 for communication with dynamoDB
import boto3
from boto3.dynamodb.conditions import Key, Attr

from fastapi import Security, Depends, FastAPI, HTTPException
from fastapi.security.api_key import APIKeyQuery, APIKeyCookie, APIKeyHeader, APIKey
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi

from starlette.status import HTTP_403_FORBIDDEN
from starlette.responses import RedirectResponse, JSONResponse

# parameters for authentication
API_KEY = "123abc"
API_KEY_NAME = "access_token"
COOKIE_DOMAIN = "localtest.me"

api_key_query = APIKeyQuery(name=API_KEY_NAME, auto_error=False)
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)
api_key_cookie = APIKeyCookie(name=API_KEY_NAME, auto_error=False)

async def get_api_key(
    api_key_query: str = Security(api_key_query),
    api_key_header: str = Security(api_key_header),
    api_key_cookie: str = Security(api_key_cookie),
):

    if api_key_query == API_KEY:
        return api_key_query
    elif api_key_header == API_KEY:
        return api_key_header
    elif api_key_cookie == API_KEY:
        return api_key_cookie
    else:
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN, detail="Could not validate credentials"
        )

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

# Route to the homepage - does not require authentication
@app.get("/")
async def homepage():
    return "Welcome to API homepage!"

# Logout page to delete cookies and block user from accessing the data
@app.get("/logout")
async def route_logout_and_remove_cookie():
    response = RedirectResponse(url="/")
    response.delete_cookie(API_KEY_NAME, domain=COOKIE_DOMAIN)
    return response

# Access the Swagger//Documentation page 
# To access - with token - http://localtest.me:8000/documentation?access_token=123abc
@app.get("/documentation", tags=["documentation"])
async def get_documentation(api_key: APIKey = Depends(get_api_key)):
    response = get_swagger_ui_html(openapi_url="/openapi.json", title="docs")
    response.set_cookie(
        API_KEY_NAME,
        value=api_key,
        domain=COOKIE_DOMAIN,
        httponly=True,
        max_age=1800,
        expires=1800,
    )
    return response

@app.get("/openapi.json", tags=["documentation"])
async def get_open_api_endpoint(api_key: APIKey = Depends(get_api_key)):
    response = JSONResponse(
        get_openapi(title="FastAPI security test", version=1, routes=app.routes)
    )
    return response

def query_from_stock_name(name, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('Data_Store')
    response = table.query(
        KeyConditionExpression=Key('name').eq(name)
    )
    return response['Items']

# Route for getting data by stock name
@app.get("/databyname")
async def dataPage(stockName: str):
    stocks = query_from_stock_name(stockName)
    return stocks

def query_from_type(stocktype, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Data_Store')
    response = table.scan(
        FilterExpression=Attr('type').eq(stocktype)
    )
    return response['Items']

# Route for getting data by stock type
@app.get("/databytype")
async def dataPage(stocktype: str):
    stocks = query_from_type(stocktype)
    return stocks

def query_date(stocktype, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Data_Store')
    response = table.scan(
        FilterExpression=Attr('date').eq(stocktype)
    )
    return response['Items']

# Route for getting data by stock date
@app.get("/databydate")
async def dataPage(stocktype: str):
    stocks = query_date(stocktype)
    return stocks