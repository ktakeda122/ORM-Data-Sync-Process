from sqlalchemy import Column, Integer, String, DateTime, Float, Date, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.automap import automap_base
from db import source_engine, target_engine

# --- Source (MySQL) Models ---
# from Sakila DB
SourceBase = automap_base()
SourceBase.prepare(autoload_with=source_engine)

SakilaFilm = SourceBase.classes.film
SakilaCategory = SourceBase.classes.category
SakilaActor = SourceBase.classes.actor
SakilaInventory = SourceBase.classes.inventory
SakilaRental = SourceBase.classes.rental
SakilaPayment = SourceBase.classes.payment
SakilaCustomer = SourceBase.classes.customer
SakilaStore = SourceBase.classes.store
SakilaAddress = SourceBase.classes.address
SakilaCity = SourceBase.classes.city
SakilaCountry = SourceBase.classes.country
SakilaFilmActor = SourceBase.classes.film_actor
SakilaFilmCategory = SourceBase.classes.film_category
SakilaStaff = SourceBase.classes.staff


# --- Target (SQLite) Models ---
TargetBase = declarative_base()

class DimDate(TargetBase):
    __tablename__ = 'dim_date'
    # date_key format: YYYYMMDD
    date_key = Column(Integer, primary_key=True)
    date = Column(Date)
    year = Column(Integer)
    quarter = Column(Integer)
    month = Column(Integer)
    day_of_month = Column(Integer)
    day_of_week = Column(Integer) # 0=Monday, 6=Sunday
    is_weekend = Column(Boolean)

class DimFilm(TargetBase):
    __tablename__ = 'dim_film'
    
    # surrogate key (*_key)
    film_key = Column(Integer, primary_key=True, autoincrement=True)
    # natural key (*_id)
    film_id = Column(Integer, nullable=False) 
    title = Column(String(255))
    rating = Column(String(10))
    length = Column(Integer)
    language = Column(String(20))
    release_year = Column(Integer)
    last_update = Column(DateTime)

class DimCustomer(TargetBase):
    __tablename__ = 'dim_customer'
    customer_key = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(Integer, unique=True)
    first_name = Column(String(45))
    last_name = Column(String(45))
    email = Column(String(50))
    active = Column(Integer)
    # Combine address information and store it here
    city = Column(String(50))
    country = Column(String(50))
    last_update = Column(DateTime)

class DimStore(TargetBase):
    __tablename__ = 'dim_store'
    store_key = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column(Integer, unique=True)
    # Combine address information
    city = Column(String(50))
    country = Column(String(50))
    last_update = Column(DateTime)

class DimActor(TargetBase):
    __tablename__ = 'dim_actor'
    actor_key = Column(Integer, primary_key=True, autoincrement=True)
    actor_id = Column(Integer, unique=True)
    first_name = Column(String(45))
    last_name = Column(String(45))
    last_update = Column(DateTime)

class DimCategory(TargetBase):
    __tablename__ = 'dim_category'
    category_key = Column(Integer, primary_key=True, autoincrement=True)
    category_id = Column(Integer, unique=True)
    name = Column(String(25))
    last_update = Column(DateTime)

class BridgeFilmActor(TargetBase):
    __tablename__ = 'bridge_film_actor'
    film_key = Column(Integer, primary_key=True)
    actor_key = Column(Integer, primary_key=True)

class BridgeFilmCategory(TargetBase):
    __tablename__ = 'bridge_film_category'
    film_key = Column(Integer, primary_key=True)
    category_key = Column(Integer, primary_key=True)

class FactRental(TargetBase):
    __tablename__ = 'fact_rental'
    fact_rental_key = Column(Integer, primary_key=True, autoincrement=True)
    rental_id = Column(Integer, unique=True)
    
    # Generate keys based on timestamps
    date_key_rented = Column(Integer, ForeignKey('dim_date.date_key'), index=True)  # FK to DimDate
    date_key_returned = Column(Integer, nullable=True)
    
    # Surrogate Keys (FKs to other dimensions)
    film_key = Column(Integer, ForeignKey('dim_film.film_key'), index=True)
    store_key = Column(Integer, ForeignKey('dim_store.store_key'), index=True)
    customer_key = Column(Integer, ForeignKey('dim_customer.customer_key'), index=True)
    
    staff_id = Column(Integer)
    rental_duration_days = Column(Integer) # Derived metric
    last_update = Column(DateTime)

class FactPayment(TargetBase):
    __tablename__ = 'fact_payment'
    fact_payment_key = Column(Integer, primary_key=True, autoincrement=True)
    payment_id = Column(Integer, unique=True)
    
    # Dates
    date_key_paid = Column(Integer, ForeignKey('dim_date.date_key'), index=True) # FK to DimDate
   
    # Keys
    customer_key = Column(Integer, ForeignKey('dim_customer.customer_key'), index=True)
    store_key = Column(Integer)
    staff_id = Column(Integer)
    
    # Metrics
    amount = Column(Float)
    last_update = Column(DateTime)


class SyncState(TargetBase):
    """
    table that records the last update date/time of each table for incremental updates.
    """
    __tablename__ = 'sync_state'
    
    table_name = Column(String(50), primary_key=True)
    last_sync_timestamp = Column(DateTime)


def create_target_tables():
    TargetBase.metadata.create_all(target_engine)