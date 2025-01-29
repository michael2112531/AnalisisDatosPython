import pandas as pd
from sqlalchemy import create_engine
import logging
import sys
from config import DATABASE_CONFIG, CSV_FILES, LOG_FILE 

#CONFIG DE LOGGING

logging.basicConfig(
    filename= LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s -%(levelname)s - %(message)s'


)

def create_db_engine(config): 
    """
    crea una conexion a la base de datos de MySQL.
    """

    try:
        
            engine = create_engine(
                f"mysql+mysqlconnector://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}")
            logging.info("conexion establecida a base de datos")

            return engine
    except Exception as e:
        logging.error(f"Error al conectar a la base de datos:{e}")  
        sys.exit(1)  
        

def read_csv(file_path):
    """
    """
    try:
        df=pd.read_csv(file_path)
        logging.info(f"Archivo{file_path} leido correctamente")
        return df  
    except Exception as e:
            logging.error(f"Error al leer el archivo{file_path}")
            sys.exit(1) 


def transform_department(df):
    """
    """
    if df['department_name'].duplicated().any():
        logging.warning("hay departamentos duplicados")
    return df


def transform_customers(df):
    """
    """
    df['customer_email']=df['customer_email'].str.lower()

    if df[['customer_fname','customer_lname','customer_email']].isnull().any().any():
        logging.error('Datos faltantes en el dataframe de customers')
        sys.exit(1)
    return df

def transform_categories(df,departments_df):
     """
     """
     valid_ids = set(departments_df['departments_id'])
     if not df['category_department_id'].isin(valid_ids).all():
        logging.error("hay category_departaments_id que no existen en departments")
        sys.exit(1)
     return df


def transform_products(df,categories_df):
     """
     """
     valid_ids= set(categories_df['category_id'])  
     if not df['product_category_id'].isin(valid_ids).all():
          logging.error("Hay product_category_id que no existen en categories")
          sys.exit(1)
     return df

def transform_orders(df, customers_df):
    """
    """

    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    if df['order_date'].isnull().any():
        logging.error("Hay valores invalidos en order_date")
        sys.exit(1)
    
    valid_ids = set(customers_df['customer_id'])
    if not df['order_customer_id'].isin(valid_ids).all():
        logging.error("Hay order_customer_id que no existen en customers")
        sys.exit(1)
   


def main():
    engine = create_db_engine(DATABASE_CONFIG)

    departments_df = read_csv(CSV_FILES['departments'])
    departments_df = transform_department(departments_df)

    customers_df = read_csv(CSV_FILES['customers'])
    customers_df = transform_customers(customers_df)

    categories_df = read_csv(CSV_FILES['categories'])
    categories_df = transform_categories(categories_df, departments_df)

    products_df = read_csv(CSV_FILES['products'])
    products_df = transform_products(products_df, categories_df)

    orders_df = read_csv(CSV_FILES['orders'])
    orders_df = transform_orders(orders_df, customers_df)

    order_items_df = read_csv(CSV_FILES['order_items'])
    order_items_df transfor_order_items(order_items_df,orders_df, products_df)

if __name__ == "__main__":
    main()




