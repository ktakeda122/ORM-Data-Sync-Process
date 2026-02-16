import datetime
from sqlalchemy.orm import Session
from sqlalchemy import func
from db import get_source_session, get_target_session
from models import (
    create_target_tables, SyncState,
    # Source
    SakilaFilm, SakilaCustomer, SakilaAddress, SakilaCity, SakilaCountry,
    SakilaStore, SakilaRental, SakilaInventory, SakilaStaff, SakilaCategory, SakilaActor, SakilaPayment,
    SakilaFilmActor, SakilaFilmCategory, 
    # Target
    DimDate, DimFilm, DimCustomer, DimStore, DimActor, DimCategory, BridgeFilmActor, BridgeFilmCategory, FactRental, FactPayment
)

# --- Helper Functions ---

def generate_date_key(dt):
    """Generates an integer in the format YYYYMMDD from a datetime object"""
    if dt is None:
        return None
    return int(dt.strftime('%Y%m%d'))

def get_surrogate_key_map(session, Model, natural_key_col, surrogate_key_col):
    """
    To speed up fact creation, create a dictionary of Natural Key (ID) -> Surrogate Key (Key).
    e.g.: {1: 1001, 2: 1002, ...} (film_id -> film_key)
    """
    results = session.query(getattr(Model, natural_key_col), getattr(Model, surrogate_key_col)).all()
    return {row[0]: row[1] for row in results}

def update_sync_state(session, table_name, timestamp):
    """
    helper function to update (or create) the sync time for a specified table
    """
    state = session.query(SyncState).get(table_name)
    if not state:
        state = SyncState(table_name=table_name)
        session.add(state)
    state.last_sync_timestamp = timestamp
    session.flush()


# --- ETL Functions ---

def load_dim_date(target_session: Session, start_year=2000, end_year=2030):
    """
    DimDate generation logic
    """
    print("Generating DimDate...")
    
    start_date = datetime.date(start_year, 1, 1)
    end_date = datetime.date(end_year, 12, 31)
    delta = datetime.timedelta(days=1)
    
    current_date = start_date
    batch = []
    
    # Check existing dates (prevent duplicate registration)
    existing_keys = {d[0] for d in target_session.query(DimDate.date_key).all()}
    
    while current_date <= end_date:
        d_key = int(current_date.strftime('%Y%m%d'))
        
        if d_key not in existing_keys:
            is_weekend = current_date.weekday() >= 5 # 5=Sat, 6=Sun
            
            dim_date = DimDate(
                date_key=d_key,
                date=current_date,
                year=current_date.year,
                quarter=(current_date.month - 1) // 3 + 1,
                month=current_date.month,
                day_of_month=current_date.day,
                day_of_week=current_date.weekday(),
                is_weekend=is_weekend
            )
            batch.append(dim_date)
        
        current_date += delta
        
        # Commit every 1000 records to save memory
        if len(batch) >= 1000:
            target_session.add_all(batch)
            target_session.commit()
            batch = []
            
    if batch:
        target_session.add_all(batch)
        target_session.commit()
    print("DimDate generation complete.")


# --- Synchronization Function---

def sync_dim_film(source, target):
    """Differential synchronization of Film table"""
    table_name = 'dim_film'
    
    # 1. Get the last updated date and time
    state = target.query(SyncState).get(table_name)
    last_sync = state.last_sync_timestamp if state else datetime.datetime.min
    
    print(f"Syncing Films since {last_sync}...")
    
    # 2. Get the difference from Source
    changes = source.query(SakilaFilm).filter(SakilaFilm.last_update > last_sync).all()
    if not changes:
        return

    # 3. Upsert
    count = 0
    for src in changes:
        # Existing Check
        existing = target.query(DimFilm).filter_by(film_id=src.film_id).first()
        
        if existing:
            # Update
            existing.title = src.title
            existing.rating = src.rating
            existing.length = src.length
            existing.last_update = datetime.datetime.now()
        else:
            # Insert
            new_obj = DimFilm(
                film_id=src.film_id,
                title=src.title,
                rating=src.rating,
                length=src.length,
                last_update=datetime.datetime.now()
            )
            target.add(new_obj)
        count += 1

    # 4. State Update and Commit
    update_sync_state(target, table_name, datetime.datetime.now())
    target.commit()
    print(f" -> Processed {count} films.")

def sync_dim_customer(source, target):
    """
    Differential synchronization of Customer table
    Join Customer -> Address -> City -> Country in MySQL
    """
    table_name = 'dim_customer'
    
    # 1. Get the last updated date and time
    state = target.query(SyncState).get(table_name)
    last_sync = state.last_sync_timestamp if state else datetime.datetime.min
    
    print(f"Syncing Customers since {last_sync}...")
    
    # 2. Get the difference from Source
    # extract the related address information based on the last_update of the Customer table, 
    changes = source.query(
        SakilaCustomer, SakilaCity.city, SakilaCountry.country
    ).join(SakilaAddress, SakilaCustomer.address_id == SakilaAddress.address_id)\
     .join(SakilaCity, SakilaAddress.city_id == SakilaCity.city_id)\
     .join(SakilaCountry, SakilaCity.country_id == SakilaCountry.country_id)\
     .filter(SakilaCustomer.last_update > last_sync).all()
     
    if not changes:
        return

    # 3. Upsert
    count = 0
    for cust_obj, city_name, country_name in changes:
        # Searching for existing records
        existing = target.query(DimCustomer).filter_by(customer_id=cust_obj.customer_id).first()
        
        if existing:
            # Update
            existing.first_name = cust_obj.first_name
            existing.last_name = cust_obj.last_name
            existing.email = cust_obj.email
            existing.active = cust_obj.active
            existing.city = city_name       
            existing.country = country_name 
            existing.last_update = datetime.datetime.now()
        else:
            # Insert: New Members
            new_cust = DimCustomer(
                customer_id=cust_obj.customer_id,
                first_name=cust_obj.first_name,
                last_name=cust_obj.last_name,
                email=cust_obj.email,
                active=cust_obj.active,
                city=city_name,
                country=country_name,
                last_update=datetime.datetime.now()
            )
            target.add(new_cust)
        count += 1

    # 4. State Update and Commit
    update_sync_state(target, table_name, datetime.datetime.now())
    target.commit()
    print(f" -> Processed {count} customers.")

def sync_dim_store(source, target):
    """
    Store table synchronization
    Join Store -> Address -> City -> Country
    """
    table_name = 'dim_store'
    
    # 1. Get the last updated date and time
    state = target.query(SyncState).get(table_name)
    last_sync = state.last_sync_timestamp if state else datetime.datetime.min
    
    print(f"Syncing Stores since {last_sync}...")
    
    # 2. Get the difference from Source
    # Store -> Address -> City -> Country
    changes = source.query(
        SakilaStore, SakilaCity.city, SakilaCountry.country
    ).join(SakilaAddress, SakilaStore.address_id == SakilaAddress.address_id)\
     .join(SakilaCity, SakilaAddress.city_id == SakilaCity.city_id)\
     .join(SakilaCountry, SakilaCity.country_id == SakilaCountry.country_id)\
     .filter(SakilaStore.last_update > last_sync).all()
     
    if not changes:
        return

    # 3. Upsert
    count = 0
    for store_obj, city_name, country_name in changes:
        existing = target.query(DimStore).filter_by(store_id=store_obj.store_id).first()
        
        if existing:
            # Update
            existing.city = city_name
            existing.country = country_name
            existing.last_update = datetime.datetime.now()
        else:
            # Insert
            new_store = DimStore(
                store_id=store_obj.store_id,
                city=city_name,
                country=country_name,
                last_update=datetime.datetime.now()
            )
            target.add(new_store)
        count += 1

    # 4. state update and commit
    update_sync_state(target, table_name, datetime.datetime.now())
    target.commit()
    print(f" -> Processed {count} stores.")

def sync_dim_actor(source, target):
    table_name = 'dim_actor'
    state = target.query(SyncState).get(table_name)
    last_sync = state.last_sync_timestamp if state else datetime.datetime.min
    print(f"Syncing Actors since {last_sync}...")
    
    changes = source.query(SakilaActor).filter(SakilaActor.last_update > last_sync).all()
    if not changes: return

    count = 0
    for src in changes:
        existing = target.query(DimActor).filter_by(actor_id=src.actor_id).first()
        if existing:
            existing.first_name = src.first_name
            existing.last_name = src.last_name
            existing.last_update = datetime.datetime.now()
        else:
            new_obj = DimActor(
                actor_id=src.actor_id,
                first_name=src.first_name,
                last_name=src.last_name,
                last_update=datetime.datetime.now()
            )
            target.add(new_obj)
        count += 1
    
    update_sync_state(target, table_name, datetime.datetime.now())
    target.commit()
    print(f" -> Processed {count} actors.")

def sync_dim_category(source, target):
    table_name = 'dim_category'
    state = target.query(SyncState).get(table_name)
    last_sync = state.last_sync_timestamp if state else datetime.datetime.min
    print(f"Syncing Categories since {last_sync}...")
    
    changes = source.query(SakilaCategory).filter(SakilaCategory.last_update > last_sync).all()
    if not changes: return

    count = 0
    for src in changes:
        existing = target.query(DimCategory).filter_by(category_id=src.category_id).first()
        if existing:
            existing.name = src.name
            existing.last_update = datetime.datetime.now()
        else:
            new_obj = DimCategory(
                category_id=src.category_id,
                name=src.name,
                last_update=datetime.datetime.now()
            )
            target.add(new_obj)
        count += 1
        
    update_sync_state(target, table_name, datetime.datetime.now())
    target.commit()
    print(f" -> Processed {count} categories.")

def sync_bridge_tables(source, target):
    """
    Syncing Bridge tables
    simply perform "delete all -> insert all" each time.
    """
    print("Syncing Bridge Tables...")
    
    # 1. Deleting existing data
    target.query(BridgeFilmActor).delete()
    target.query(BridgeFilmCategory).delete()
    
    # 2. Get Key Map (for ID -> Key conversion)
    film_map = get_surrogate_key_map(target, DimFilm, 'film_id', 'film_key')
    actor_map = get_surrogate_key_map(target, DimActor, 'actor_id', 'actor_key')
    category_map = get_surrogate_key_map(target, DimCategory, 'category_id', 'category_key')
    
    # 3. Loading Film-Actor
    fa_links = source.query(SakilaFilmActor).all()
    batch_fa = []
    for link in fa_links:
        f_key = film_map.get(link.film_id)
        a_key = actor_map.get(link.actor_id)
        if f_key and a_key:
            batch_fa.append(BridgeFilmActor(film_key=f_key, actor_key=a_key))
    
    if batch_fa:
        target.add_all(batch_fa)
    print(f" -> Loaded {len(batch_fa)} film-actor links.")

    # 4. Loading Film-Category
    fc_links = source.query(SakilaFilmCategory).all()
    batch_fc = []
    for link in fc_links:
        f_key = film_map.get(link.film_id)
        c_key = category_map.get(link.category_id)
        if f_key and c_key:
            batch_fc.append(BridgeFilmCategory(film_key=f_key, category_key=c_key))
            
    if batch_fc:
        target.add_all(batch_fc)
    print(f" -> Loaded {len(batch_fc)} film-category links.")
    
    target.commit()

def sync_fact_rental(source, target):
    """
    Rental table differential synchronization
    * Fact tables require foreign key resolution
    """
    table_name = 'fact_rental'
    
    state = target.query(SyncState).get(table_name)
    last_sync = state.last_sync_timestamp if state else datetime.datetime.min
    
    print(f"Syncing Rentals since {last_sync}...")
    
    # for rental, either a return (update) or a new rental (insert)
    # use JOIN to get all the information at once
    changes = source.query(
        SakilaRental, SakilaInventory.film_id
    ).join(SakilaInventory, SakilaRental.inventory_id == SakilaInventory.inventory_id)\
     .filter(SakilaRental.last_update > last_sync).all()
     
    if not changes:
        return

    # Re-acquire keymap for speedup
    film_map = get_surrogate_key_map(target, DimFilm, 'film_id', 'film_key')
    cust_map = get_surrogate_key_map(target, DimCustomer, 'customer_id', 'customer_key')

    count = 0
    for r_obj, film_id in changes:
        fact = target.query(FactRental).filter_by(rental_id=r_obj.rental_id).first()
        
        # Period Calculation
        duration = 0
        if r_obj.return_date:
            duration = (r_obj.return_date - r_obj.rental_date).days

        if fact:
            # Update
            fact.date_key_returned = generate_date_key(r_obj.return_date)
            fact.rental_duration_days = duration
            fact.last_update = datetime.datetime.now()
        else:
            # Insert
            fact = FactRental(
                rental_id=r_obj.rental_id,
                date_key_rented=generate_date_key(r_obj.rental_date),
                date_key_returned=generate_date_key(r_obj.return_date),
                rental_duration_days=duration,
                staff_id=r_obj.staff_id,
                film_key=film_map.get(film_id),         # conversion
                customer_key=cust_map.get(r_obj.customer_id), # conversion
                last_update=datetime.datetime.now()
            )
            target.add(fact)
        count += 1
        
    update_sync_state(target, table_name, datetime.datetime.now())
    target.commit()
    print(f" -> Processed {count} rentals.")

def sync_fact_payment(source, target):
    """
    Sales data synchronization
    Join Payment -> Staff -> Store to get store_id
    """
    table_name = 'fact_payment'
    state = target.query(SyncState).get(table_name)
    last_sync = state.last_sync_timestamp if state else datetime.datetime.min
    print(f"Syncing Payments since {last_sync}...")

    # Since Payment does not have store_id, identify the store via Staff.
    changes = source.query(
        SakilaPayment, SakilaStaff.store_id
    ).join(SakilaStaff, SakilaPayment.staff_id == SakilaStaff.staff_id)\
     .filter(SakilaPayment.last_update > last_sync).all()
    
    if not changes: return

    # Speed-up map
    cust_map = get_surrogate_key_map(target, DimCustomer, 'customer_id', 'customer_key')
    store_map = get_surrogate_key_map(target, DimStore, 'store_id', 'store_key')

    count = 0
    for p_obj, store_id in changes:
        fact = target.query(FactPayment).filter_by(payment_id=p_obj.payment_id).first()
        
        # Mapping
        cust_key = cust_map.get(p_obj.customer_id)
        st_key = store_map.get(store_id)
        date_key = generate_date_key(p_obj.payment_date)

        if fact:
            fact.amount = float(p_obj.amount)
            fact.date_key_paid = date_key
            fact.customer_key = cust_key
            fact.store_key = st_key
            fact.last_update = datetime.datetime.now()
        else:
            fact = FactPayment(
                payment_id=p_obj.payment_id,
                amount=float(p_obj.amount),
                staff_id=p_obj.staff_id,
                date_key_paid=date_key,
                customer_key=cust_key,
                store_key=st_key,
                last_update=datetime.datetime.now()
            )
            target.add(fact)
        count += 1

    update_sync_state(target, table_name, datetime.datetime.now())
    target.commit()
    print(f" -> Processed {count} payments.")


# --- Main Logic Integration ---


def init_db():
    """CLI: init - Create SQLite Table"""
    print("Initializing analytics database...")
    create_target_tables()
    print("Database initialized.")
    session = get_target_session()
    load_dim_date(session)
    session.close()

def full_load():
    """
    CLI: full-load
    Deletes all existing data and reloads all data from Source
    """
    source_session = get_source_session()
    target_session = get_target_session()

    current_time = datetime.datetime.now()
    
    try:
        print(" - Clearing existing data...")
        target_session.query(FactRental).delete()
        target_session.query(FactPayment).delete()

        target_session.query(DimStore).delete()
        target_session.query(DimCustomer).delete()
        target_session.query(DimFilm).delete()
        target_session.query(DimActor).delete() 
        target_session.query(DimCategory).delete()

        target_session.query(SyncState).delete()
        target_session.commit()
        print(" - Data cleared. Starting fresh load...")

        # Load Dims
        sync_dim_film(source_session, target_session)
        sync_dim_customer(source_session, target_session)
        sync_dim_store(source_session, target_session)
        sync_dim_actor(source_session, target_session) 
        sync_dim_category(source_session, target_session)

        # Load Facts
        sync_fact_rental(source_session, target_session)
        sync_fact_payment(source_session, target_session)

        sync_bridge_tables(source_session, target_session)

        target_session.commit()
        print("Full load completed.")


    except Exception as e:
        target_session.rollback()
        print(f"Error during full load: {e}")
    finally:
        source_session.close()
        target_session.close()


def incremental_load():
    """CLI: incremental"""
    source_session = get_source_session()
    target_session = get_target_session()
    
    print("Starting Incremental Load...")

    try:
        # Call the synchronization process for each table
        sync_dim_film(source_session, target_session)
        sync_dim_customer(source_session, target_session)
        sync_dim_store(source_session, target_session)
        sync_dim_actor(source_session, target_session) 
        sync_dim_category(source_session, target_session)
        
        # Fact is executed last because it depends on Dimension
        sync_fact_rental(source_session, target_session)
        sync_fact_payment(source_session, target_session)

        sync_bridge_tables(source_session, target_session)
        
        print("Incremental Load Completed.")
        
    except Exception as e:
        print(f"Error during incremental load: {e}")
        target_session.rollback()
    finally:
        source_session.close()
        target_session.close()



def validate_data():
    """CLI: validate"""
    source = get_source_session()
    target = get_target_session()
    
    print("Validating data consistency (Source vs Target)...")
    print("-" * 60)
    print(f"{'Table / Metric':<20} | {'Source':<10} | {'Target':<10} | {'Status':<10}")
    print("-" * 60)
    
    issues_found = False
    
    try:
        # 1. Count Checks
        # ---------------------------------------------------------
        checks = [
            ("Films", SakilaFilm.film_id, DimFilm.film_key),
            ("Customers", SakilaCustomer.customer_id, DimCustomer.customer_key),
            ("Stores", SakilaStore.store_id, DimStore.store_key),
            ("Actors", SakilaActor.actor_id, DimActor.actor_key),
            ("Categories", SakilaCategory.category_id, DimCategory.category_key),
            ("Rentals", SakilaRental.rental_id, FactRental.fact_rental_key),
            ("Payments", SakilaPayment.payment_id, FactPayment.fact_payment_key),
        ]
        
        for name, src_col, tgt_col in checks:
            src_count = source.query(func.count(src_col)).scalar() or 0
            tgt_count = target.query(func.count(tgt_col)).scalar() or 0
            
            status = "OK" if src_count == tgt_count else "FAIL"
            if status == "FAIL": issues_found = True
            
            print(f"{name:<20} | {src_count:<10} | {tgt_count:<10} | {status:<10}")

        # 2. Revenue Check
        # ---------------------------------------------------------
        # Payment Amount Sum
        src_rev = source.query(func.sum(SakilaPayment.amount)).scalar() or 0.0
        tgt_rev = target.query(func.sum(FactPayment.amount)).scalar() or 0.0
        
        # Since comparison of floating-point numbers, the difference is used to determine the result.
        diff = abs(float(src_rev) - float(tgt_rev))
        rev_status = "OK" if diff < 0.01 else "FAIL"
        if rev_status == "FAIL": issues_found = True
        
        print("-" * 60)
        print(f"{'Total Revenue':<20} | {float(src_rev):<10.2f} | {float(tgt_rev):<10.2f} | {rev_status:<10}")
        print("-" * 60)
        
        if issues_found:
            print("\n[!] Validation FAILED: Inconsistencies found.")
        else:
            print("\n[+] Validation PASSED: All data matches.")
            
    except Exception as e:
        print(f"Error during validation: {e}")
    finally:
        source.close()
        target.close()