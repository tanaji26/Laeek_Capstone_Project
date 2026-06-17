"""
base_data_generation.py
------------------------
Generates synthetic Indian e-commerce datasets.

Airflow-compatible: all logic wrapped in run() function.
Call run() from PythonOperator — no sys.exit(), no top-level side effects.
"""

import os
import pandas as pd
import numpy as np
from datetime import date


def run():
    """
    Entry point for Airflow PythonOperator.
    Generates all four synthetic datasets and saves to /data/ as CSV.
    Raises Exception on failure (Airflow marks task failed automatically).
    """
    DATA_DIR = "/data"
    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"Data directory ready: {DATA_DIR}")

    rng = np.random.default_rng(42)

    # ── Products ──────────────────────────────────────────────────────────────
    products_raw_data = [
        ("PRD-001","Samsung Galaxy M34 5G","Electronics",13500,18999),
        ("PRD-002","boAt Rockerz 450 Bluetooth Headphones","Electronics",700,1299),
        ("PRD-003","Redmi 12C Smartphone","Electronics",6800,9499),
        ("PRD-004","HP 15s Intel Core i3 Laptop","Electronics",32000,44990),
        ("PRD-005","Zebronics ZEB-Sound Bomb Q3 TWS","Electronics",900,1799),
        ("PRD-006","Sony WH-1000XM5 Headphones","Electronics",22000,29990),
        ("PRD-007","Realme Narzo N55","Electronics",8200,11999),
        ("PRD-008","Mi 43-inch Smart TV 4X","Electronics",22500,31999),
        ("PRD-009","Canon EOS 1500D DSLR Camera","Electronics",30000,41995),
        ("PRD-010","Lenovo IdeaPad Gaming 3","Electronics",56000,74990),
        ("PRD-011","Levis 511 Slim Fit Jeans","Apparel",1100,2499),
        ("PRD-012","Allen Solly Mens Formal Shirt","Apparel",550,1199),
        ("PRD-013","W Womens Kurta Set","Apparel",700,1599),
        ("PRD-014","Nike Dri-FIT Training T-Shirt","Apparel",800,1795),
        ("PRD-015","Biba Anarkali Salwar Suit","Apparel",1400,3299),
        ("PRD-016","US Polo Assn Polo T-Shirt","Apparel",600,1399),
        ("PRD-017","Libas Womens Floral Maxi Dress","Apparel",500,1099),
        ("PRD-018","Roadster Mens Bomber Jacket","Apparel",1200,2799),
        ("PRD-019","Jockey Womens Cotton Leggings","Apparel",280,699),
        ("PRD-020","Peter England Mens Chinos","Apparel",900,1999),
        ("PRD-021","Tata Salt Iodised Rock Salt 1kg","Grocery",16,24),
        ("PRD-022","Aashirvaad Atta Whole Wheat Flour 5kg","Grocery",185,265),
        ("PRD-023","Amul Gold Full Cream Milk 1L","Grocery",52,72),
        ("PRD-024","Fortune Sunflower Refined Oil 1L","Grocery",115,160),
        ("PRD-025","Haldirams Bhujia Sev 400g","Grocery",95,140),
        ("PRD-026","India Gate Basmati Rice Classic 5kg","Grocery",390,550),
        ("PRD-027","Tata Tea Gold Leaf Tea 500g","Grocery",180,260),
        ("PRD-028","Nescafe Classic Instant Coffee 100g","Grocery",180,265),
        ("PRD-029","MDH Chana Masala Spice Mix 100g","Grocery",50,80),
        ("PRD-030","Parle-G Original Glucose Biscuits 800g","Grocery",60,90),
        ("PRD-031","Wakefit Orthopaedic Memory Foam Mattress Queen","Furniture",7500,12999),
        ("PRD-032","Nilkamal Plastic 4-Seater Dining Table","Furniture",4200,6999),
        ("PRD-033","Durian Leatherette 3-Seater Sofa","Furniture",9500,15999),
        ("PRD-034","Pepperfry Engineered Wood Study Table","Furniture",2800,4999),
        ("PRD-035","Godrej Interio Metal Folding Chair","Furniture",1600,2799),
        ("PRD-036","Alex Daisy Engineered Wood Wardrobe","Furniture",5000,8499),
        ("PRD-037","Hometown Sheesham Wood Coffee Table","Furniture",3200,5499),
        ("PRD-038","Cello Novelty Large Plastic Cabinet","Furniture",1900,3299),
        ("PRD-039","Wakefit Recliner Chair","Furniture",6500,10999),
        ("PRD-040","Kurlon King Size Bonnell Spring Mattress","Furniture",11000,18499),
        ("PRD-041","Cosco Football Size 5","Sports",450,799),
        ("PRD-042","Nivia Encounter Basketball Size 7","Sports",600,1099),
        ("PRD-043","Yonex ZR 100 Light Badminton Racquet","Sports",350,649),
        ("PRD-044","Boldfit Resistance Band Set 5-piece","Sports",300,599),
        ("PRD-045","Adidas Runfalcon 3.0 Running Shoes","Sports",2800,4999),
        ("PRD-046","Strauss Yoga Mat 6mm","Sports",480,899),
        ("PRD-047","SG Cricket Bat English Willow Grade 3","Sports",2200,3999),
        ("PRD-048","Cockatoo CFD-05 Adjustable Dumbbell Pair 5kg","Sports",1100,1999),
        ("PRD-049","Decathlon Btwin 20-inch Kids Cycle","Sports",3500,5999),
        ("PRD-050","Li-Ning Smash XP 900 Table Tennis Bat","Sports",750,1399),
    ]
    products_data = pd.DataFrame(
        products_raw_data,
        columns=["product_id","product_name","product_category",
                 "cost_price_per_unit","retail_price_per_unit"]
    )

    # ── Customers ─────────────────────────────────────────────────────────────
    N_CUST = 200
    first_names = ["Aarav","Aditya","Akash","Amit","Ananya","Anjali","Arjun","Aryan",
                   "Deepika","Divya","Gaurav","Ishaan","Kavya","Kiran","Manav","Meera",
                   "Neha","Nikhil","Pooja","Priya","Rahul","Rajesh","Riya","Rohit",
                   "Sakshi","Sanjay","Shreya","Shubham","Sneha","Tanvi","Varun","Vikram",
                   "Vishal","Yash","Zara","Sunita","Ramesh","Geeta","Harsha","Lakshmi"]
    last_names  = ["Agarwal","Bhat","Chauhan","Desai","Garg","Gupta","Iyer","Jain",
                   "Joshi","Kapoor","Khanna","Kumar","Malhotra","Mehta","Mishra","Nair",
                   "Pandey","Patel","Pillai","Rao","Reddy","Saxena","Shah","Sharma",
                   "Shukla","Singh","Sinha","Srivastava","Tiwari","Varma","Verma","Yadav"]
    cities_regions = [
        ("Mumbai","West"),("Delhi","North"),("Bengaluru","South"),("Hyderabad","South"),
        ("Chennai","South"),("Kolkata","East"),("Pune","West"),("Ahmedabad","West"),
        ("Jaipur","North"),("Lucknow","North"),("Bhopal","Central"),("Nagpur","Central"),
        ("Surat","West"),("Vadodara","West"),("Indore","Central"),("Patna","East"),
        ("Bhubaneswar","East"),("Kochi","South"),("Chandigarh","North"),("Guwahati","East"),
    ]

    city_arr, region_arr = zip(*cities_regions)
    cust_idx  = rng.integers(0, len(cities_regions), N_CUST)
    fn_idx    = rng.integers(0, len(first_names), N_CUST)
    ln_idx    = rng.integers(0, len(last_names),  N_CUST)
    ages      = rng.integers(18, 65, N_CUST)
    genders   = rng.choice(["Male","Female","Other"], N_CUST, p=[0.50,0.47,0.03])
    is_prime  = rng.choice([True, False], N_CUST, p=[0.35, 0.65])
    start_ord = date(2024, 1, 1).toordinal()
    end_ord   = date(2026, 5, 31).toordinal()
    rand_ords = rng.integers(0, end_ord - start_ord, N_CUST) + start_ord
    prime_start_dates = pd.to_datetime([date.fromordinal(int(o)) for o in rand_ords])
    prime_end_dates   = prime_start_dates + pd.DateOffset(years=1)

    names  = [f"{first_names[f]} {last_names[l]}" for f, l in zip(fn_idx, ln_idx)]
    emails = [f"{n.lower().replace(' ','.')}{rng.integers(1,999)}@example.com" for n in names]
    phones = [f"+91 {rng.integers(70000,99999):05d} {rng.integers(10000,99999):05d}" for _ in range(N_CUST)]

    customer = pd.DataFrame({
        "customer_id":        [f"Cust-{i:04d}" for i in range(1, N_CUST+1)],
        "customer_name":      names,
        "customer_email":     emails,
        "customer_contact_no": phones,
        "customer_location":  [city_arr[i] for i in cust_idx],
        "customer_region":    [region_arr[i] for i in cust_idx],
        "customer_age":       ages,
        "customer_gender":    genders,
        "is_prime_customer":  is_prime,
        "prime_start_date":   np.where(is_prime, prime_start_dates.astype(str), pd.NaT),
        "prime_end_date":     np.where(is_prime, prime_end_dates.astype(str),   pd.NaT),
    })

    # ── Orders ────────────────────────────────────────────────────────────────
    n_orders = 10000
    cat_weights = products_data["product_category"].map(
        {"Grocery":0.35,"Apparel":0.25,"Electronics":0.20,"Sports":0.12,"Furniture":0.08}
    ).values
    cat_weights = cat_weights / cat_weights.sum()
    prod_idx  = rng.choice(len(products_data), n_orders, p=cat_weights)
    cust_idx2 = rng.integers(0, N_CUST, n_orders)

    order_ords     = rng.integers(date(2024,1,1).toordinal(), date(2026,5,31).toordinal(), n_orders)
    order_dates    = pd.to_datetime([date.fromordinal(int(o)) for o in order_ords])
    delivery_dates = order_dates + pd.to_timedelta(rng.integers(1, 10, n_orders), unit="D")
    statuses       = rng.choice(["Delivered","Cancelled","Returned"], n_orders, p=[0.8,0.12,0.08])

    selected_prods = products_data.iloc[prod_idx].reset_index(drop=True)
    qtys           = rng.integers(1, 6, n_orders)
    unit_prices    = selected_prods["retail_price_per_unit"].values
    unit_price_tax = np.round(unit_prices * 0.18, 2)
    total_amount   = np.round(unit_prices * qtys, 2)

    order_customer_ids = customer.iloc[cust_idx2]["customer_id"].values
    cust_prime_lookup  = customer.set_index("customer_id")[
        ["is_prime_customer","prime_start_date","prime_end_date"]
    ]
    order_dates_series = pd.Series(order_dates.date)
    is_active_prime    = np.zeros(n_orders, dtype=bool)

    for i, (cid, odate) in enumerate(zip(order_customer_ids, order_dates_series)):
        row = cust_prime_lookup.loc[cid]
        if row["is_prime_customer"] and pd.notna(row["prime_start_date"]) and pd.notna(row["prime_end_date"]):
            p_start = pd.to_datetime(row["prime_start_date"]).date()
            p_end   = pd.to_datetime(row["prime_end_date"]).date()
            is_active_prime[i] = (p_start <= odate <= p_end)

    discount_pct = np.where(
        is_active_prime,
        rng.integers(8, 16, n_orders) / 100,
        rng.integers(2,  8, n_orders) / 100
    )
    discount_pct   = np.round(discount_pct, 2)
    total_discount = np.round(total_amount * discount_pct, 2)
    final_amount   = np.round(total_amount - total_discount, 2)

    payment_methods  = np.empty(n_orders, dtype=object)
    low_mask         = final_amount <= 5000
    high_mask        = final_amount > 5000
    payment_methods[low_mask]  = rng.choice(["UPI","Cash on Delivery"], size=low_mask.sum(), p=[0.75,0.25])
    payment_methods[high_mask] = rng.choice(["Credit Card","Debit Card","Net Banking","EMI"], size=high_mask.sum(), p=[0.40,0.35,0.15,0.10])
    payment_statuses = np.where(np.isin(statuses, ["Cancelled"]), "Refunded", "Paid")

    orders = pd.DataFrame({
        "order_id":         [f"ORD-{i:05d}" for i in range(1, n_orders+1)],
        "product_id":       selected_prods["product_id"].values,
        "customer_id":      customer.iloc[cust_idx2]["customer_id"].values,
        "order_date":       order_dates.date,
        "customer_location": customer.iloc[cust_idx2]["customer_location"].values,
        "customer_region":  customer.iloc[cust_idx2]["customer_region"].values,
        "product_name":     selected_prods["product_name"].values,
        "product_category": selected_prods["product_category"].values,
        "delivery_date":    delivery_dates.date,
        "order_status":     statuses,
        "order_qty":        qtys,
        "unit_price":       unit_prices,
        "unit_price_tax":   unit_price_tax,
        "total_amount":     total_amount,
        "discount_pct":     discount_pct,
        "total_discount":   total_discount,
        "final_amount":     final_amount,
        "currency":         "INR",
        "payment_method":   payment_methods,
        "payment_status":   payment_statuses,
    })

    # ── Feedback ──────────────────────────────────────────────────────────────
    delivered_orders = orders[orders["order_status"] == "Delivered"].sample(
        frac=0.70, random_state=42
    ).reset_index(drop=True)
    N_FB = len(delivered_orders)

    review_templates = {
        "Great product, very happy with the purchase!":          5,
        "Good quality for the price.":                           4,
        "Delivered on time, product as described.":              5,
        "Excellent build quality, highly recommend.":            5,
        "Decent product but packaging could be better.":         3,
        "Satisfied with the purchase overall.":                  4,
        "Product works well, good value for money.":             3,
        "Quality is average, expected better.":                  3,
        "Not satisfied, product did not match description.":     2,
        "Poor quality, returned the item.":                      1,
        "Amazing product! Will buy again.":                      5,
        "Fast delivery and good product.":                       4,
        "Okay product, nothing special.":                        2,
        "Worst purchase ever, complete waste of money.":         1,
        "Very good product, exceeded expectations.":             5,
    }
    template_weights = [0.12,0.09,0.09,0.10,0.07,0.08,0.07,0.07,0.06,0.04,
                        0.06,0.05,0.04,0.02,0.04]
    chosen_templates = rng.choice(list(review_templates.keys()), size=N_FB, p=template_weights)
    review_scores    = [review_templates[t] for t in chosen_templates]
    fb_dates = pd.to_datetime(delivered_orders["delivery_date"]) + pd.to_timedelta(
        rng.integers(1, 15, N_FB), unit="D"
    )
    feedback = pd.DataFrame({
        "feedback_id":   [f"FB-{i:05d}" for i in range(1, N_FB+1)],
        "order_id":      delivered_orders["order_id"].values,
        "review_date":   fb_dates.dt.date,
        "product_review": chosen_templates,
        "review_score":  review_scores,
    })

    # ── Save CSVs ─────────────────────────────────────────────────────────────
    orders.to_csv(os.path.join(DATA_DIR, "orders_data.csv"),   index=False)
    customer.to_csv(os.path.join(DATA_DIR, "customer_data.csv"), index=False)
    products_data.to_csv(os.path.join(DATA_DIR, "products_data.csv"), index=False)
    feedback.to_csv(os.path.join(DATA_DIR, "feedback_data.csv"), index=False)
    print(f"All files saved to: {DATA_DIR}")


if __name__ == "__main__":
    run()