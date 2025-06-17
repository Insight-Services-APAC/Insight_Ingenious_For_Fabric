import datetime
import fabric.functions as fn
import logging

udf = fn.UserDataFunctions()

@udf.function()
def insert_log_message(warehousename: str) -> str:
    logging.info('Python UDF trigger function processed a request.')
    logging.info(warehousename)
    return f"Welcome to Fabric Functions, {warehousename}, at {datetime.datetime.now()}!"