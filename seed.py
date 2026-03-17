"""seed.py — Seed FarmTracker with demo data. Run: python seed.py"""
import sys, os, random
sys.path.insert(0, os.path.dirname(__file__))
from db import db_conn, init_db
from auth import hash_password
from datetime import date, timedelta

PW = hash_password("password123")
CROPS = [("sweet_potato","Sweet Potato","tuber",""),("pepper_chili","Pepper (Chili)","vegetable","hot variety"),
         ("bee_farming","Bee Farming","apiculture","hives tracked separately"),("sorghum","Sorghum","cereal","drought tolerant"),
         ("banana","Banana","fruit","plantain type"),("pigeon_peas","Pigeon Peas","legume","")]
FIELDS = [("North Field",1.2,-13.00,34.50,"loam","sweet_potato","2026-01-20","2026-03-18","planted"),
          ("South Field",0.8,-13.01,34.52,"clay","pepper_chili","2026-02-10","2026-04-10","planted"),
          ("East Block",1.5,-13.02,34.54,"loam","sorghum","2026-01-05","2026-03-29","planted"),
          ("West Ridge",0.6,-12.99,34.48,"sandy","banana","2025-11-01","2026-03-27","planted"),
          ("Dambo Plot",0.4,-13.03,34.55,"silt","pigeon_peas","2026-02-01","2026-03-23","planted"),
          ("Hive Meadow",0.2,-13.05,34.56,"loam",None,None,None,"idle")]
INVENTORY = [("NPK Fertilizer","fertilizer","kg",12,25,350,"Farmers World"),("Urea","fertilizer","kg",80,20,280,"Farmers World"),
             ("Sweet Potato Vine","seed","bundles",200,50,50,"Chitedze Agri"),("Chili Seeds","seed","g",500,100,2,"Seed Co"),
             ("Sorghum Seeds","seed","kg",30,10,180,"Seed Co"),("Banana Suckers","seed","pieces",150,30,75,"Local nursery"),
             ("Pigeon Pea Seeds","seed","kg",25,5,220,"Seed Co"),("Dimethoate","pesticide","L",5,5,1800,"Agri-inputs Ltd"),
             ("Mancozeb","pesticide","kg",8,3,900,"Agri-inputs Ltd"),("Hand Sprayer 16L","equipment","pieces",4,1,8500,"Hardware Plus"),
             ("Hessian Bags","equipment","pieces",300,50,85,"Shoprite")]
HIVES = [("HV-01","2025-06-01","2026-02-28","healthy",1),("HV-02","2025-07-15","2026-02-28","healthy",1),
         ("HV-03","2025-08-10","2026-02-20","weak",0),("HV-04","2025-09-01","2026-03-05","healthy",1),
         ("HV-05","2025-05-20","2026-03-01","healthy",1)]
BUYERS = ["Limbe Market","Shoprite","ADMARC","Wenela Market","Direct Sale","Chipiku Stores"]
PRICES = {"sweet_potato":120,"pepper_chili":370,"bee_farming":1200,"sorghum":70,"banana":120,"pigeon_peas":280}

def seed():
    print("Initialising schema..."); init_db()
    with db_conn() as conn:
        if not conn.execute("SELECT id FROM users LIMIT 1").fetchone():
            conn.executemany("INSERT INTO users(name,email,password_hash,role)VALUES(?,?,?,?)",
                [("Admin User","admin@farm.mw",PW,"admin"),("James Mwale","manager@farm.mw",PW,"manager"),
                 ("Grace Banda","worker1@farm.mw",PW,"worker"),("Peter Chirwa","worker2@farm.mw",PW,"worker")])
            print("  4 users created.")
        existing = {r[0] for r in conn.execute("SELECT slug FROM crops").fetchall()}
        new = [(s,n,c,no) for s,n,c,no in CROPS if s not in existing]
        if new: conn.executemany("INSERT INTO crops(slug,display_name,category,notes)VALUES(?,?,?,?)",new); print(f"  {len(new)} crops.")
        crop_map = {r["slug"]:r["id"] for r in conn.execute("SELECT id,slug FROM crops").fetchall()}
        ef = {r[0] for r in conn.execute("SELECT name FROM fields").fetchall()}
        for name,size,lat,lon,soil,cslug,pd,hd,st in FIELDS:
            if name not in ef:
                conn.execute("INSERT INTO fields(name,size_ha,gps_lat,gps_lon,soil_type,crop_id,planting_date,expected_harvest_date,status)VALUES(?,?,?,?,?,?,?,?,?)",
                    (name,size,lat,lon,soil,crop_map.get(cslug),pd,hd,st))
        field_map = {r["name"]:r["id"] for r in conn.execute("SELECT id,name FROM fields").fetchall()}
        print(f"  {len(FIELDS)} fields.")
        if not conn.execute("SELECT id FROM inventory_items LIMIT 1").fetchone():
            conn.executemany("INSERT INTO inventory_items(name,category,unit,qty_on_hand,reorder_threshold,cost_per_unit,supplier)VALUES(?,?,?,?,?,?,?)",INVENTORY)
            print(f"  {len(INVENTORY)} inventory items.")
        if not conn.execute("SELECT id FROM beehives LIMIT 1").fetchone():
            hfid = field_map.get("Hive Meadow")
            for code,queen,insp,health,prod in HIVES:
                conn.execute("INSERT INTO beehives(hive_code,location_field_id,queen_date,last_inspection_date,health_status,is_producing)VALUES(?,?,?,?,?,?)",(code,hfid,queen,insp,health,prod))
                hid=conn.execute("SELECT id FROM beehives WHERE hive_code=?",(code,)).fetchone()[0]
                conn.execute("INSERT INTO honey_harvests(hive_id,date,qty_liters,quality)VALUES(?,?,?,?)",(hid,"2026-02-15",round(random.uniform(2,8),1),"A"))
            print(f"  {len(HIVES)} hives.")
        crop_ids=list(crop_map.values()); field_ids=list(field_map.values())
        if not conn.execute("SELECT id FROM harvests LIMIT 1").fetchone():
            for _ in range(30):
                d=(date.today()-timedelta(days=random.randint(0,120))).isoformat()
                conn.execute("INSERT INTO harvests(field_id,crop_id,date,qty,unit,quality,storage_location)VALUES(?,?,?,?,?,?,?)",
                    (random.choice(field_ids),random.choice(crop_ids),d,round(random.uniform(50,800),1),"kg",random.choice(["A","A","B","C"]),"Main Store"))
            print("  30 harvests.")
        if not conn.execute("SELECT id FROM sales LIMIT 1").fetchone():
            for i in range(40):
                sd=(date.today()-timedelta(days=random.randint(0,90))).isoformat()
                slug=random.choice(list(PRICES.keys())); cid=crop_map.get(slug,crop_ids[0])
                qty=round(random.uniform(20,300),1); price=PRICES[slug]; total=round(qty*price,2)
                cur=conn.execute("INSERT INTO sales(crop_id,buyer,qty,unit,price_per_unit,total,date,payment_status)VALUES(?,?,?,?,?,?,?,?)",
                    (cid,random.choice(BUYERS),qty,"kg",price,total,sd,random.choice(["paid","paid","pending","invoiced"])))
                sid=cur.lastrowid; conn.execute("UPDATE sales SET invoice_id=? WHERE id=?",(f"INV-{sd.replace('-','')}-{sid:04d}",sid))
            print("  40 sales.")
        if not conn.execute("SELECT id FROM expenses LIMIT 1").fetchone():
            for i in range(20):
                d=(date.today()-timedelta(days=random.randint(0,90))).isoformat()
                conn.execute("INSERT INTO expenses(date,category,amount,description)VALUES(?,?,?,?)",
                    (d,random.choice(["labour","inputs","equipment","transport","other"]),round(random.uniform(500,15000),2),f"Expense #{i+1}"))
            print("  20 expenses.")
        if not conn.execute("SELECT id FROM tasks LIMIT 1").fetchone():
            uids=[r[0] for r in conn.execute("SELECT id FROM users WHERE role IN('worker','manager')").fetchall()]
            for i in range(15):
                d=(date.today()-timedelta(days=random.randint(0,30))).isoformat()
                conn.execute("INSERT INTO tasks(field_id,assigned_user_id,task_type,date,hours,cost,status)VALUES(?,?,?,?,?,?,?)",
                    (random.choice(field_ids),random.choice(uids) if uids else None,
                     random.choice(["planting","weeding","irrigation","spraying","harvesting"]),
                     d,round(random.uniform(2,8),1),round(random.uniform(500,3000),2),random.choice(["pending","in_progress","done"])))
            print("  15 tasks.")
    print("\nSeed complete!\nLogin: admin@farm.mw / password123\nRun:   python app.py")

if __name__ == "__main__": seed()
