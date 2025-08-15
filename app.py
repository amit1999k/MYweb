
import streamlit as st
import pandas as pd
import numpy as np
import io, zipfile, datetime as dt
from io import BytesIO

st.set_page_config(page_title="GSTify Web – GST Data Processor", layout="wide")

# ------------------------------
# Session State Initialization
# ------------------------------
if "global_data" not in st.session_state:
    st.session_state.global_data = {
        "b2cs": [],       # list of DataFrames
        "hsn": [],        # list of DataFrames (B2C HSN)
        "b2b": [],        # list of DataFrames
        "hsn_b2b": []     # list of DataFrames (B2B HSN)
    }

if "processed_files_tracker" not in st.session_state:
    st.session_state.processed_files_tracker = {
        "amazon": set(),
        "flipkart": set(),
        "jiomart": set(),
        "meesho_sales": set(),
        "meesho_return": set(),
        "glowroad": set(),
        "b2b_template": set(),
        "amazon_b2b": set(),
        "b2c_other": set(),
    }

if "eco_gstins" not in st.session_state:
    st.session_state.eco_gstins = {
        "Amazon": "",
        "Flipkart": "",
        "Jiomart": "",
        "Meesho": "",
        "Glowroad": ""
    }

if "user_business_state" not in st.session_state:
    st.session_state.user_business_state = "Madhya Pradesh"

if "user_business_state_mapped" not in st.session_state:
    st.session_state.user_business_state_mapped = ""

# ------------------------------
# Constants & Helpers
# ------------------------------
STATE_NAME_MAPPING = {
    'Andaman & Nicobar Islands': '35-Andaman & Nicobar Islands',
    'Andaman And Nicobar Islands': '35-Andaman & Nicobar Islands',
    'Andaman and Nicobar Islands': '35-Andaman & Nicobar Islands',
    'Andhra Pradesh': '37-Andhra Pradesh',
    'Arunachal Pradesh': '12-Arunachal Pradesh',
    'Assam': '18-Assam',
    'Bihar': '10-Bihar',
    'Chandigarh': '04-Chandigarh',
    'Chattisgarh': '22-Chhattisgarh',
    'Chhattisgarh': '22-Chhattisgarh',
    'Dadra & Nagar Haveli': '26-Dadra & Nagar Haveli',
    'Dadra And Nagar Haveli And Daman And Diu': '26-Dadra & Nagar Haveli',
    'The Dadra And Nagar Haveli': '26-Dadra & Nagar Haveli',
    'Dadra And Nagar Haveli': '26-Dadra & Nagar Haveli',
    'Daman And Diu': '25-Daman & Diu',
    'Daman & Diu': '25-Daman & Diu',
    'Daman and Diu': '25-Daman & Diu',
    'Delhi': '07-Delhi',
    'New Delhi': '07-Delhi',
    'Foreign Country': '96-Foreign Country',
    'Goa': '30-Goa',
    'Gujarat': '24-Gujarat',
    'Haryana': '06-Haryana',
    'Himachal Pradesh': '02-Himachal Pradesh',
    'Jammu & Kashmir': '01-Jammu & Kashmir',
    'Jammu And Kashmir': '01-Jammu & Kashmir',
    'Jammu and Kashmir': '01-Jammu & Kashmir',
    'Jharkhand': '20-Jharkhand',
    'Karnataka': '29-Karnataka',
    'Kerala': '32-Kerala',
    'Ladakh': '38-Ladakh',
    'Lakshdweep': '31-Lakshdweep',
    'Madhya Pradesh': '23-Madhya Pradesh',
    'Maharashtra': '27-Maharashtra',
    'Manipur': '14-Manipur',
    'Meghalaya': '17-Meghalaya',
    'Mizoram': '15-Mizoram',
    'Nagaland': '13-Nagaland',
    'Odisha': '21-Odisha',
    'Other Territory': '97-Other Territory',
    'Puducherry': '34-Puducherry',
    'Pondicherry': '34-Puducherry',
    'Punjab': '03-Punjab',
    'Rajasthan': '08-Rajasthan',
    'Sikkim': '11-Sikkim',
    'Tamil Nadu': '33-Tamil Nadu',
    'Telangana': '36-Telangana',
    'Tripura': '16-Tripura',
    'Uttar Pradesh': '09-Uttar Pradesh',
    'Uttarakhand': '05-Uttarakhand',
    'West Bengal': '19-West Bengal'
}

INDIAN_STATES_LIST = [
    'Andaman And Nicobar Islands', 'Andhra Pradesh', 'Arunachal Pradesh', 'Assam', 'Bihar',
    'Chandigarh', 'Chhattisgarh', 'Dadra And Nagar Haveli And Daman And Diu', 'Delhi', 'Goa',
    'Gujarat', 'Haryana', 'Himachal Pradesh', 'Jammu And Kashmir', 'Jharkhand', 'Karnataka',
    'Kerala', 'Ladakh', 'Lakshadweep', 'Madhya Pradesh', 'Maharashtra', 'Manipur',
    'Meghalaya', 'Mizoram', 'Nagaland', 'Odisha', 'Other Territory', 'Puducherry',
    'Punjab', 'Rajasthan', 'Sikkim', 'Tamil Nadu', 'Telangana', 'Tripura',
    'Uttar Pradesh', 'Uttarakhand', 'West Bengal'
]

def custom_round_gst_rate(rate):
    if pd.isna(rate):
        return 0
    try:
        rate = float(rate)
        return int(np.floor(rate + 0.5))
    except (ValueError, TypeError):
        return 0

def _read_any(uploaded):
    name = uploaded.name
    if name.lower().endswith(".csv"):
        return pd.read_csv(uploaded), name
    else:
        return pd.read_excel(uploaded), name

def calculate_gst_amounts(df, taxable_value_col, gst_rate_col, state_col_for_igst_cgst_sgst=None):
    df[taxable_value_col] = pd.to_numeric(df[taxable_value_col], errors='coerce').fillna(0)
    df[gst_rate_col] = pd.to_numeric(df[gst_rate_col], errors='coerce').fillna(0)

    df['calculated_tax'] = df[taxable_value_col] * (df[gst_rate_col] / 100)
    df['calculated_tax'] = df['calculated_tax'].fillna(0)

    user_state = st.session_state.user_business_state_mapped
    if state_col_for_igst_cgst_sgst and user_state:
        df[state_col_for_igst_cgst_sgst] = df[state_col_for_igst_cgst_sgst].astype(str).str.strip().str.title()
        df['Mapped_State_For_GST_Calc'] = df[state_col_for_igst_cgst_sgst].replace(STATE_NAME_MAPPING)
        df['is_intra_state'] = df['Mapped_State_For_GST_Calc'].apply(lambda x: pd.notna(x) and x.upper() == user_state.upper())
        df['IGST'] = df.apply(lambda row: row['calculated_tax'] if not row['is_intra_state'] else 0, axis=1)
        df['CGST'] = df.apply(lambda row: row['calculated_tax'] / 2 if row['is_intra_state'] else 0, axis=1)
        df['SGST'] = df['CGST']
        df = df.drop(columns=['Mapped_State_For_GST_Calc','is_intra_state'])
    else:
        df['IGST'] = df['calculated_tax']
        df['CGST'] = 0
        df['SGST'] = 0

    df['IGST'] = pd.to_numeric(df['IGST'], errors='coerce').fillna(0)
    df['CGST'] = pd.to_numeric(df['CGST'], errors='coerce').fillna(0)
    df['SGST'] = pd.to_numeric(df['SGST'], errors='coerce').fillna(0)
    return df.drop(columns=['calculated_tax'])

def _concat_if_any(lst):
    return pd.concat(lst, ignore_index=True) if lst else pd.DataFrame()

def _round_rate_cols(df):
    if 'GST Rate' in df.columns:
        df['GST Rate'] = pd.to_numeric(df['GST Rate'], errors='coerce').fillna(0).apply(custom_round_gst_rate).astype(int)
    if 'Rate' in df.columns:
        df['Rate'] = pd.to_numeric(df['Rate'], errors='coerce').fillna(0).apply(custom_round_gst_rate).astype(int)
    return df

def _map_state_cols(df):
    for col in ['State', 'Place Of Supply']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.title().replace(STATE_NAME_MAPPING)
            df[col] = df[col].apply(lambda x: x if x in STATE_NAME_MAPPING.values() or pd.isna(x) else f"Unmapped: {x}")
    return df

def get_compiled_data(data_list, group_cols=None, agg_dict=None):
    if not data_list:
        return pd.DataFrame()
    merged_df = _concat_if_any(data_list)
    merged_df = _map_state_cols(merged_df)
    merged_df = _round_rate_cols(merged_df)
    if 'HSN' in merged_df.columns:
        merged_df['HSN'] = merged_df['HSN'].astype(str).fillna('').str.replace(r'\.0$', '', regex=True).str.strip()
    if group_cols and agg_dict:
        merged_df = merged_df.groupby(group_cols, as_index=False).agg(agg_dict)
    # normalize numerics
    for col in merged_df.columns:
        if pd.api.types.is_numeric_dtype(merged_df[col]):
            merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce').fillna(0)
    if 'Invoice Date' in merged_df.columns:
        merged_df['Invoice Date'] = merged_df['Invoice Date'].astype(str)
    return merged_df

def to_excel_bytes(df, sheet_name="Sheet1"):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
    return output.getvalue()

def format_invoice_date(val):
    try:
        if isinstance(val, (int, float)):
            return pd.to_datetime(val, unit='D', origin=pd.Timestamp('1899-12-30')).strftime('%d-%b-%Y')
        return pd.to_datetime(val).strftime('%d-%b-%Y')
    except Exception:
        return pd.NA

# ------------------------------
# Sidebar: Business Settings
# ------------------------------
with st.sidebar:
    st.title("Business Settings")
    st.selectbox("Your Business State", INDIAN_STATES_LIST, index=INDIAN_STATES_LIST.index(st.session_state.user_business_state) if st.session_state.user_business_state in INDIAN_STATES_LIST else 0, key="user_business_state")
    st.session_state.user_business_state_mapped = STATE_NAME_MAPPING.get(st.session_state.user_business_state, "")
    st.markdown("---")
    st.subheader("E-commerce Operator GSTINs")
    for op in st.session_state.eco_gstins.keys():
        st.session_state.eco_gstins[op] = st.text_input(f"{op} GSTIN", st.session_state.eco_gstins[op])

    st.markdown("---")
    if st.button("Clear All Loaded Data", type="primary"):
        st.session_state.global_data = {"b2cs": [], "hsn": [], "b2b": [], "hsn_b2b": []}
        for k in st.session_state.processed_files_tracker:
            st.session_state.processed_files_tracker[k] = set()
        st.success("All loaded data cleared.")

st.title("GSTify Web")
st.caption("GST data processing for Amazon / Flipkart / Jiomart / Meesho / Glowroad / B2C (Other) and B2B, with merged reports.")

# ------------------------------
# Upload & Process
# ------------------------------
tabs = st.tabs([
    "Amazon", "Flipkart", "Jiomart", "Meesho Sales", "Meesho Return",
    "Glowroad", "B2C (Other)",
    "B2B Template", "Amazon B2B"
])

with tabs[0]:
    st.subheader("Upload Amazon")
    files = st.file_uploader("Upload Amazon Excel/CSV", type=["xlsx","xls","csv"], accept_multiple_files=True, key="amazon_u")
    if st.button("Process Amazon"):
        if not files: st.warning("Upload at least one file."); st.stop()
        for up in files:
            if up.name in st.session_state.processed_files_tracker["amazon"]:
                continue
            df, fname = _read_any(up)
            req = ['Ship To State','Tax Exclusive Gross','Cgst Rate','Sgst Rate','Igst Rate','Hsn/sac','Quantity','Invoice Amount']
            if not all(c in df.columns for c in req):
                st.error(f"{fname}: missing columns {set(req)-set(df.columns)}"); continue
            df['Tax Exclusive Gross'] = pd.to_numeric(df['Tax Exclusive Gross'], errors='coerce').fillna(0)
            df['Cgst Rate'] = pd.to_numeric(df['Cgst Rate'], errors='coerce').fillna(0)
            df['Sgst Rate'] = pd.to_numeric(df['Sgst Rate'], errors='coerce').fillna(0)
            df['Igst Rate'] = pd.to_numeric(df['Igst Rate'], errors='coerce').fillna(0)
            df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(0)
            df['Invoice Amount'] = pd.to_numeric(df['Invoice Amount'], errors='coerce').fillna(0)
            df['Ship To State'] = df['Ship To State'].astype(str).str.title()
            df = df[df['Tax Exclusive Gross'] != 0].copy()
            if df.empty: st.warning(f"{fname}: no rows after filtering Taxable>0"); continue
            df['Rate'] = (df[['Cgst Rate','Sgst Rate','Igst Rate']].sum(axis=1)*100).apply(custom_round_gst_rate)
            b2cs = df.groupby(['Ship To State','Rate'])['Tax Exclusive Gross'].sum().reset_index()
            b2cs.columns = ['State','GST Rate','Taxable Value']
            st.session_state.global_data["b2cs"].append(b2cs)

            df = calculate_gst_amounts(df,'Tax Exclusive Gross','Rate','Ship To State')
            df['Total Rate'] = df['Rate']
            hsn = df.groupby([df['Hsn/sac'].astype(str).str.strip(), df['Total Rate'].astype(int)]).agg(
                Quantity=('Quantity','sum'),
                Invoice_Amount=('Invoice Amount','sum'),
                Taxable_Value=('Tax Exclusive Gross','sum'),
                IGST=('IGST','sum'),
                CGST=('CGST','sum'),
                SGST=('SGST','sum')
            ).reset_index()
            hsn.columns = ['HSN','GST Rate','Quantity','Invoice Amount','Taxable Value','IGST','CGST','SGST']
            hsn['Source_Platform'] = 'Amazon'
            st.session_state.global_data["hsn"].append(hsn)
            st.session_state.processed_files_tracker["amazon"].add(up.name)
        st.success("Amazon processed.")

with tabs[1]:
    st.subheader("Upload Flipkart (GSTR-1 workbook)")
    files = st.file_uploader("Upload Flipkart Excel", type=["xlsx","xls"], accept_multiple_files=True, key="flipkart_u")
    if st.button("Process Flipkart"):
        if not files: st.warning("Upload at least one file."); st.stop()
        if not st.session_state.user_business_state:
            st.warning("Set your Business State in the sidebar for accurate processing."); st.stop()
        for up in files:
            if up.name in st.session_state.processed_files_tracker["flipkart"]:
                continue
            xl = pd.ExcelFile(up)
            if "Section 7(A)(2) in GSTR-1" in xl.sheet_names:
                df7a = xl.parse("Section 7(A)(2) in GSTR-1")
                need = ["Aggregate Taxable Value Rs.","CGST %","SGST/UT %"]
                if all(c in df7a.columns for c in need):
                    df7a["Aggregate Taxable Value Rs."] = pd.to_numeric(df7a["Aggregate Taxable Value Rs."], errors='coerce').fillna(0)
                    df7a["CGST %"] = pd.to_numeric(df7a["CGST %"], errors='coerce').fillna(0)
                    df7a["SGST/UT %"] = pd.to_numeric(df7a["SGST/UT %"], errors='coerce').fillna(0)
                    b2cs_7a = pd.DataFrame({
                        "State":[st.session_state.user_business_state.title()]*len(df7a),
                        "GST Rate":(df7a["CGST %"]+df7a["SGST/UT %"]).apply(custom_round_gst_rate),
                        "Taxable Value":df7a["Aggregate Taxable Value Rs."]
                    })
                    st.session_state.global_data["b2cs"].append(b2cs_7a[["State","GST Rate","Taxable Value"]])
            if "Section 7(B)(2) in GSTR-1" in xl.sheet_names:
                df7b = xl.parse("Section 7(B)(2) in GSTR-1")
                need = ["Aggregate Taxable Value Rs.","IGST %","Delivered State (PoS)"]
                if all(c in df7b.columns for c in need):
                    df7b["Aggregate Taxable Value Rs."] = pd.to_numeric(df7b["Aggregate Taxable Value Rs."], errors='coerce').fillna(0)
                    df7b["IGST %"] = pd.to_numeric(df7b["IGST %"], errors='coerce').fillna(0)
                    out = pd.DataFrame({
                        "State": df7b["Delivered State (PoS)"].astype(str).str.title(),
                        "GST Rate": df7b["IGST %"].apply(custom_round_gst_rate),
                        "Taxable Value": df7b["Aggregate Taxable Value Rs."]
                    })
                    st.session_state.global_data["b2cs"].append(out[["State","GST Rate","Taxable Value"]])
            if "Section 12 in GSTR-1" in xl.sheet_names:
                dfx = xl.parse("Section 12 in GSTR-1")
                need = ["HSN Number","Total Quantity in Nos.","Total Taxable Value Rs.","IGST Amount Rs.","CGST Amount Rs.","SGST Amount Rs."]
                if all(c in dfx.columns for c in need):
                    dfx["Total Quantity in Nos."] = pd.to_numeric(dfx["Total Quantity in Nos."], errors='coerce').fillna(0)
                    dfx["Total Taxable Value Rs."] = pd.to_numeric(dfx["Total Taxable Value Rs."], errors='coerce').fillna(0)
                    dfx["IGST Amount Rs."] = pd.to_numeric(dfx["IGST Amount Rs."], errors='coerce').fillna(0)
                    dfx["CGST Amount Rs."] = pd.to_numeric(dfx["CGST Amount Rs."], errors='coerce').fillna(0)
                    dfx["SGST Amount Rs."] = pd.to_numeric(dfx["SGST Amount Rs."], errors='coerce').fillna(0)
                    hsn = pd.DataFrame({
                        "HSN": dfx["HSN Number"].astype(str).str.strip(),
                        "Quantity": dfx["Total Quantity in Nos."],
                        "Taxable Value": dfx["Total Taxable Value Rs."],
                        "IGST": dfx["IGST Amount Rs."],
                        "CGST": dfx["CGST Amount Rs."],
                        "SGST": dfx["SGST Amount Rs."]
                    })
                    hsn["Invoice Amount"] = hsn["Taxable Value"] + hsn["IGST"] + hsn["CGST"] + hsn["SGST"]
                    total_tax = hsn["IGST"] + hsn["CGST"] + hsn["SGST"]
                    hsn["GST Rate"] = np.where(hsn["Taxable Value"]!=0, (total_tax/hsn["Taxable Value"])*100, 0).round().astype(int)
                    hsn = hsn[["HSN","GST Rate","Quantity","Invoice Amount","Taxable Value","IGST","CGST","SGST"]]
                    hsn["Source_Platform"] = "Flipkart"
                    st.session_state.global_data["hsn"].append(hsn)
            st.session_state.processed_files_tracker["flipkart"].add(up.name)
        st.success("Flipkart processed.")

with tabs[2]:
    st.subheader("Upload Jiomart")
    files = st.file_uploader("Upload Jiomart Excel/CSV", type=["xlsx","xls","csv"], accept_multiple_files=True, key="jiomart_u")
    if st.button("Process Jiomart"):
        if not files: st.warning("Upload at least one file."); st.stop()
        for up in files:
            if up.name in st.session_state.processed_files_tracker["jiomart"]:
                continue
            df, fname = _read_any(up)
            req = ["Customer's Billing State",'IGST Rate','CGST Rate','SGST Rate (or UTGST as applicable)',
                   'Taxable Value (Final Invoice Amount -Taxes)','HSN Code','Item Quantity',
                   'Final Invoice Amount (Offer Price minus Seller Coupon Amount)','IGST Amount','CGST Amount',
                   'SGST Amount (Or UTGST as applicable)']
            if not all(c in df.columns for c in req):
                st.error(f"{fname}: missing columns {set(req)-set(df.columns)}"); continue
            for c in ['IGST Rate','CGST Rate','SGST Rate (or UTGST as applicable)',
                      'Taxable Value (Final Invoice Amount -Taxes)','Item Quantity',
                      'Final Invoice Amount (Offer Price minus Seller Coupon Amount)','IGST Amount','CGST Amount','SGST Amount (Or UTGST as applicable)']:
                df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
            df["Customer's Billing State"] = df["Customer's Billing State"].astype(str).str.title()
            df['Rate'] = (df[['IGST Rate','CGST Rate','SGST Rate (or UTGST as applicable)']].sum(axis=1)).apply(custom_round_gst_rate)
            b2cs = df.groupby(["Customer's Billing State",'Rate'])['Taxable Value (Final Invoice Amount -Taxes)'].sum().reset_index()
            b2cs.columns = ['State','GST Rate','Taxable Value']
            st.session_state.global_data["b2cs"].append(b2cs)
            df = calculate_gst_amounts(df,'Taxable Value (Final Invoice Amount -Taxes)','Rate',"Customer's Billing State")
            hsn = df.groupby([df['HSN Code'].astype(str).str.strip(), df['Rate'].astype(int)]).agg(
                Quantity=('Item Quantity','sum'),
                Invoice_Amount=('Final Invoice Amount (Offer Price minus Seller Coupon Amount)','sum'),
                Taxable_Value=('Taxable Value (Final Invoice Amount -Taxes)','sum'),
                IGST=('IGST Amount','sum'),
                CGST=('CGST Amount','sum'),
                SGST=('SGST Amount (Or UTGST as applicable)','sum')
            ).reset_index()
            hsn.columns = ['HSN','GST Rate','Quantity','Invoice Amount','Taxable Value','IGST','CGST','SGST']
            hsn['Source_Platform'] = 'Jiomart'
            st.session_state.global_data["hsn"].append(hsn)
            st.session_state.processed_files_tracker["jiomart"].add(up.name)
        st.success("Jiomart processed.")

with tabs[3]:
    st.subheader("Upload Meesho Sales")
    files = st.file_uploader("Upload Meesho Sales Excel/CSV", type=["xlsx","xls","csv"], accept_multiple_files=True, key="meesho_s_u")
    if st.button("Process Meesho Sales"):
        if not files: st.warning("Upload at least one file."); st.stop()
        for up in files:
            if up.name in st.session_state.processed_files_tracker["meesho_sales"]:
                continue
            df, fname = _read_any(up)
            req = ['end_customer_state_new','gst_rate','total_taxable_sale_value','hsn_code','quantity','total_invoice_value']
            if not all(c in df.columns for c in req):
                st.error(f"{fname}: missing columns {set(req)-set(df.columns)}"); continue
            df['gst_rate'] = pd.to_numeric(df['gst_rate'], errors='coerce').fillna(0).apply(custom_round_gst_rate)
            df['total_taxable_sale_value'] = pd.to_numeric(df['total_taxable_sale_value'], errors='coerce').fillna(0)
            df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)
            df['total_invoice_value'] = pd.to_numeric(df['total_invoice_value'], errors='coerce').fillna(0)
            df['end_customer_state_new'] = df['end_customer_state_new'].astype(str).str.title()
            b2cs = df.groupby(['end_customer_state_new','gst_rate'])['total_taxable_sale_value'].sum().reset_index()
            b2cs.columns = ['State','GST Rate','Taxable Value']
            st.session_state.global_data["b2cs"].append(b2cs)
            df = calculate_gst_amounts(df,'total_taxable_sale_value','gst_rate','end_customer_state_new')
            hsn = df.groupby([df['hsn_code'].astype(str).str.strip(), df['gst_rate'].astype(int)]).agg(
                Quantity=('quantity','sum'),
                Invoice_Amount=('total_invoice_value','sum'),
                Taxable_Value=('total_taxable_sale_value','sum'),
                IGST=('IGST','sum'),
                CGST=('CGST','sum'),
                SGST=('SGST','sum')
            ).reset_index()
            hsn.columns = ['HSN','GST Rate','Quantity','Invoice Amount','Taxable Value','IGST','CGST','SGST']
            hsn['Source_Platform'] = 'Meesho_Sales'
            st.session_state.global_data["hsn"].append(hsn)
            st.session_state.processed_files_tracker["meesho_sales"].add(up.name)
        st.success("Meesho Sales processed.")

with tabs[4]:
    st.subheader("Upload Meesho Return")
    files = st.file_uploader("Upload Meesho Return Excel/CSV", type=["xlsx","xls","csv"], accept_multiple_files=True, key="meesho_r_u")
    if st.button("Process Meesho Return"):
        if not files: st.warning("Upload at least one file."); st.stop()
        for up in files:
            if up.name in st.session_state.processed_files_tracker["meesho_return"]:
                continue
            df, fname = _read_any(up)
            req = ['end_customer_state_new','gst_rate','total_taxable_sale_value','hsn_code','quantity','total_invoice_value']
            if not all(c in df.columns for c in req):
                st.error(f"{fname}: missing columns {set(req)-set(df.columns)}"); continue
            df['gst_rate'] = pd.to_numeric(df['gst_rate'], errors='coerce').fillna(0).apply(custom_round_gst_rate)
            df['total_taxable_sale_value'] = pd.to_numeric(df['total_taxable_sale_value'], errors='coerce').fillna(0)
            df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)
            df['total_invoice_value'] = pd.to_numeric(df['total_invoice_value'], errors='coerce').fillna(0)
            df['end_customer_state_new'] = df['end_customer_state_new'].astype(str).str.title()
            # B2CS negate
            b2cs = df.groupby(['end_customer_state_new','gst_rate'])['total_taxable_sale_value'].sum().reset_index()
            b2cs.columns = ['State','GST Rate','Taxable Value']
            b2cs['Taxable Value'] = -b2cs['Taxable Value']
            st.session_state.global_data["b2cs"].append(b2cs)
            # HSN then negate aggregates
            df = calculate_gst_amounts(df,'total_taxable_sale_value','gst_rate','end_customer_state_new')
            hsn = df.groupby([df['hsn_code'].astype(str).str.strip(), df['gst_rate'].astype(int)]).agg(
                Quantity=('quantity','sum'),
                Invoice_Amount=('total_invoice_value','sum'),
                Taxable_Value=('total_taxable_sale_value','sum'),
                IGST=('IGST','sum'),
                CGST=('CGST','sum'),
                SGST=('SGST','sum')
            ).reset_index()
            hsn.columns = ['HSN','GST Rate','Quantity','Invoice Amount','Taxable Value','IGST','CGST','SGST']
            for c in ['Quantity','Invoice Amount','Taxable Value','IGST','CGST','SGST']:
                hsn[c] = -pd.to_numeric(hsn[c], errors='coerce').fillna(0)
            hsn['Source_Platform'] = 'Meesho_Return'
            st.session_state.global_data["hsn"].append(hsn)
            st.session_state.processed_files_tracker["meesho_return"].add(up.name)
        st.success("Meesho Return processed.")

with tabs[5]:
    st.subheader("Upload Glowroad")
    files = st.file_uploader("Upload Glowroad Excel/CSV", type=["xlsx","xls","csv"], accept_multiple_files=True, key="glowroad_u")
    if st.button("Process Glowroad"):
        if not files: st.warning("Upload at least one file."); st.stop()
        for up in files:
            if up.name in st.session_state.processed_files_tracker["glowroad"]:
                continue
            df, fname = _read_any(up)
            req = ['Base amount for GST ', 'Buyer state', 'GST %', 'Product HSN code',
                   'SGST','UTGST','CGST','IGST','Customer invoice value (GMV)']
            if not all(c in df.columns for c in req):
                st.error(f"{fname}: missing columns {set(req)-set(df.columns)}"); continue
            df['Product HSN code'] = df['Product HSN code'].astype(str).str.replace('.','', regex=False).str.replace(',','', regex=False).str.strip()
            df['Base amount for GST '] = pd.to_numeric(df['Base amount for GST '], errors='coerce').fillna(0)
            df['GST %'] = pd.to_numeric(df['GST %'], errors='coerce').fillna(0)
            for c in ['SGST','UTGST','CGST','IGST','Customer invoice value (GMV)']:
                df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
            df = df[df['Base amount for GST '] != 0].copy()
            if df.empty: st.warning(f"{fname}: no rows after filtering Taxable>0"); continue
            df['Buyer state'] = df['Buyer state'].astype(str).str.title()
            df['Rate'] = df['GST %'].apply(custom_round_gst_rate)
            b2cs = df.groupby(['Buyer state','Rate'])['Base amount for GST '].sum().reset_index()
            b2cs.columns = ['State','GST Rate','Taxable Value']
            st.session_state.global_data["b2cs"].append(b2cs)
            df['Product HSN code'] = df['Product HSN code'].astype(str).str.replace(r'\.0$','', regex=True).str.strip()
            df['Quantity'] = 1
            df = calculate_gst_amounts(df,'Base amount for GST ','Rate','Buyer state')
            hsn = df.groupby([df['Product HSN code'], df['Rate'].astype(int)]).agg(
                Quantity=('Quantity','sum'),
                Invoice_Amount=('Customer invoice value (GMV)','sum'),
                Taxable_Value=('Base amount for GST ','sum'),
                CGST=('CGST','sum'),
                SGST=('SGST','sum'),
                IGST=('IGST','sum')
            ).reset_index()
            hsn.columns = ['HSN','GST Rate','Quantity','Invoice Amount','Taxable Value','CGST','SGST','IGST']
            hsn = hsn[['HSN','GST Rate','Quantity','Invoice Amount','Taxable Value','IGST','CGST','SGST']]
            hsn['Source_Platform'] = 'Glowroad'
            st.session_state.global_data["hsn"].append(hsn)
            st.session_state.processed_files_tracker["glowroad"].add(up.name)
        st.success("Glowroad processed.")

with tabs[6]:
    st.subheader("Upload B2C (Other)")
    files = st.file_uploader("Upload B2C (Other) Excel/CSV", type=["xlsx","xls","csv"], accept_multiple_files=True, key="b2c_other_u")
    if st.button("Process B2C (Other)"):
        if not files: st.warning("Upload at least one file."); st.stop()
        for up in files:
            if up.name in st.session_state.processed_files_tracker["b2c_other"]:
                continue
            df, fname = _read_any(up)
            req = ['Place Of Supply','Rate','Taxable Value','Total Value','HSN','Total Quantity',
                   'Integrated Tax Amount','Central Tax Amount','State/UT Tax Amount','Cess Amount']
            if not all(c in df.columns for c in req):
                st.error(f"{fname}: missing columns {set(req)-set(df.columns)}"); continue
            for c in ['Rate','Taxable Value','Total Value','Total Quantity','Integrated Tax Amount','Central Tax Amount','State/UT Tax Amount','Cess Amount']:
                df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
            df = df[df['Taxable Value'] != 0].copy()
            if df.empty: st.warning(f"{fname}: no rows after filtering Taxable>0"); continue
            df['Place Of Supply'] = df['Place Of Supply'].astype(str).str.title()
            df['Rate'] = df['Rate'].apply(custom_round_gst_rate)
            b2cs = df.groupby(['Place Of Supply','Rate'])['Taxable Value'].sum().reset_index()
            b2cs.columns = ['State','GST Rate','Taxable Value']
            st.session_state.global_data["b2cs"].append(b2cs)
            hsn = df.groupby([df['HSN'].astype(str).str.strip(), df['Rate'].astype(int)]).agg(
                Quantity=('Total Quantity','sum'),
                Invoice_Amount=('Total Value','sum'),
                Taxable_Value=('Taxable Value','sum'),
                IGST=('Integrated Tax Amount','sum'),
                CGST=('Central Tax Amount','sum'),
                SGST=('State/UT Tax Amount','sum')
            ).reset_index()
            hsn.columns = ['HSN','GST Rate','Quantity','Invoice Amount','Taxable Value','IGST','CGST','SGST']
            hsn['Source_Platform'] = 'B2C_Other'
            st.session_state.global_data["hsn"].append(hsn)
            st.session_state.processed_files_tracker["b2c_other"].add(up.name)
        st.success("B2C (Other) processed.")

with tabs[7]:
    st.subheader("Import B2B Template")
    files = st.file_uploader("Upload B2B Template Excel/CSV", type=["xlsx","xls","csv"], accept_multiple_files=True, key="b2b_t_u")
    if st.button("Process B2B Template"):
        if not files: st.warning("Upload at least one file."); st.stop()
        for up in files:
            if up.name in st.session_state.processed_files_tracker["b2b_template"]:
                continue
            df, fname = _read_any(up)
            req = ['GSTIN/UIN of Recipient','Receiver Name','Invoice Number','Invoice Date','Invoice Value','Place Of Supply','Total Rate','Taxable Value','Cess Amount','HSN','Total Quantity']
            if not all(c in df.columns for c in req):
                st.error(f"{fname}: missing columns {set(req)-set(df.columns)}"); continue
            for c in ['Invoice Value','Total Rate','Taxable Value','Cess Amount','Total Quantity']:
                df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
            dff = df[df['Taxable Value'].notnull() & (df['Taxable Value']>0)].copy()
            if dff.empty: st.warning(f"{fname}: no positive Taxable Value rows"); continue
            dff['Invoice Date'] = dff['Invoice Date'].apply(format_invoice_date)
            dff = dff.dropna(subset=['Invoice Date'])
            if dff.empty: st.warning(f"{fname}: no valid Invoice Dates"); continue
            dff['Total Rate'] = dff['Total Rate'].apply(custom_round_gst_rate)
            b2b_output = pd.DataFrame({
                'GSTIN/UIN of Recipient': dff['GSTIN/UIN of Recipient'].astype(str),
                'Receiver Name': dff['Receiver Name'].astype(str),
                'Invoice Number': dff['Invoice Number'].astype(str),
                'Invoice Date': dff['Invoice Date'].astype(str),
                'Invoice Value': dff['Invoice Value'],
                'Place Of Supply': dff['Place Of Supply'].astype(str),
                'Reverse Charge':'N',
                'Applicable % of Tax Rate':'',
                'Invoice Type':'Regular',
                'E-Commerce GSTIN':'',
                'Rate': dff['Total Rate'],
                'Taxable Value': dff['Taxable Value'],
                'Cess Amount': dff['Cess Amount']
            })
            st.session_state.global_data["b2b"].append(b2b_output)
            dff = calculate_gst_amounts(dff,'Taxable Value','Total Rate','Place Of Supply')
            h = dff.groupby([dff['HSN'].astype(str).str.strip(), dff['Total Rate'].astype(int)]).agg(
                Total_Quantity=('Total Quantity','sum'),
                Total_Value=('Invoice Value','sum'),
                Taxable_Value=('Taxable Value','sum'),
                Integrated_Tax_Amount=('IGST','sum'),
                Central_Tax_Amount=('CGST','sum'),
                State_UT_Tax_Amount=('SGST','sum'),
                Cess_Amount=('Cess Amount','sum')
            ).reset_index()
            h['Description']=''; h['UQC']='PCS-PIECES'
            out = h[['HSN','Description','UQC','Total_Quantity','Total_Value','Taxable_Value','Integrated_Tax_Amount','Central_Tax_Amount','State_UT_Tax_Amount','Cess_Amount','Total Rate']].copy()
            out.columns = ['HSN','Description','UQC','Total Quantity','Total Value','Taxable Value','Integrated Tax Amount','Central Tax Amount','State/UT Tax Amount','Cess Amount','Rate']
            out['Source_Platform'] = 'B2B_Template'
            st.session_state.global_data["hsn_b2b"].append(out)
            st.session_state.processed_files_tracker["b2b_template"].add(up.name)
        st.success("B2B Template processed.")

with tabs[8]:
    st.subheader("Upload Amazon B2B")
    files = st.file_uploader("Upload Amazon B2B Excel/CSV", type=["xlsx","xls","csv"], accept_multiple_files=True, key="amazon_b2b_u")
    if st.button("Process Amazon B2B"):
        if not files: st.warning("Upload at least one file."); st.stop()
        for up in files:
            if up.name in st.session_state.processed_files_tracker["amazon_b2b"]:
                continue
            df, fname = _read_any(up)
            req = ['Customer Bill To Gstid','Buyer Name','Invoice Number','Invoice Date','Invoice Amount','Ship To State','Cgst Rate','Sgst Rate','Utgst Rate','Igst Rate','Tax Exclusive Gross','Compensatory Cess Rate','Hsn/sac','Quantity']
            if not all(c in df.columns for c in req):
                st.error(f"{fname}: missing columns {set(req)-set(df.columns)}"); continue
            for c in ['Invoice Amount','Cgst Rate','Sgst Rate','Utgst Rate','Igst Rate','Tax Exclusive Gross','Compensatory Cess Rate','Quantity']:
                df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
            dff = df[df['Tax Exclusive Gross'].notnull() & (df['Tax Exclusive Gross']>0)].copy()
            dff['Invoice Date'] = dff['Invoice Date'].apply(format_invoice_date)
            dff = dff.dropna(subset=['Invoice Date'])
            if dff.empty: st.warning(f"{fname}: no valid Invoice Dates"); continue
            dff['Rate'] = (dff[['Cgst Rate','Sgst Rate','Utgst Rate','Igst Rate']].sum(axis=1)*100).apply(custom_round_gst_rate)
            b2b_output = pd.DataFrame({
                'GSTIN/UIN of Recipient': dff['Customer Bill To Gstid'].astype(str),
                'Receiver Name': dff['Buyer Name'].astype(str),
                'Invoice Number': dff['Invoice Number'].astype(str),
                'Invoice Date': dff['Invoice Date'].astype(str),
                'Invoice Value': dff['Invoice Amount'],
                'Place Of Supply': dff['Ship To State'].astype(str),
                'Reverse Charge': 'N',
                'Applicable % of Tax Rate': '',
                'Invoice Type': 'Regular B2B',
                'E-Commerce GSTIN': '',
                'Rate': dff['Rate'],
                'Taxable Value': dff['Tax Exclusive Gross'],
                'Cess Amount': dff['Compensatory Cess Rate']
            })
            st.session_state.global_data["b2b"].append(b2b_output)
            dff = calculate_gst_amounts(dff,'Taxable Value' if 'Taxable Value' in dff.columns else 'Tax Exclusive Gross','Rate','Ship To State')
            h = dff.groupby([dff['Hsn/sac'].astype(str).str.strip(), dff['Rate'].astype(int)]).agg(
                Total_Quantity=('Quantity','sum'),
                Total_Value=('Invoice Amount','sum'),
                Taxable_Value=('Tax Exclusive Gross','sum'),
                Integrated_Tax_Amount=('IGST','sum'),
                Central_Tax_Amount=('CGST','sum'),
                State_UT_Tax_Amount=('SGST','sum'),
                Cess_Amount=('Compensatory Cess Rate','sum')
            ).reset_index()
            h['Description']=''; h['UQC']='PCS-PIECES'
            out = h[['Hsn/sac','Description','UQC','Total_Quantity','Total_Value','Taxable_Value','Integrated_Tax_Amount','Central_Tax_Amount','State_UT_Tax_Amount','Cess_Amount','Rate']].copy()
            out.columns = ['HSN','Description','UQC','Total Quantity','Total Value','Taxable Value','Integrated Tax Amount','Central Tax Amount','State/UT Tax Amount','Cess Amount','Rate']
            out['Source_Platform'] = 'Amazon_B2B'
            st.session_state.global_data["hsn_b2b"].append(out)
            st.session_state.processed_files_tracker["amazon_b2b"].add(up.name)
        st.success("Amazon B2B processed.")

# ------------------------------
# Merged Views
# ------------------------------
st.markdown("## Merged Data View")
col1, col2 = st.columns(2)
with col1:
    st.markdown("#### B2CS (State × GST Rate)")
    b2cs_df = get_compiled_data(st.session_state.global_data["b2cs"], group_cols=['State','GST Rate'], agg_dict={'Taxable Value':'sum'})
    st.dataframe(b2cs_df, use_container_width=True)
with col2:
    st.markdown("#### HSN (Sales)")
    hsn_df = get_compiled_data(st.session_state.global_data["hsn"], group_cols=['HSN','GST Rate'], agg_dict={'Quantity':'sum','Invoice Amount':'sum','Taxable Value':'sum','IGST':'sum','CGST':'sum','SGST':'sum'})
    st.dataframe(hsn_df, use_container_width=True)

col3, col4 = st.columns(2)
with col3:
    st.markdown("#### B2B Output")
    b2b_df = get_compiled_data(st.session_state.global_data["b2b"])
    desired = ['GSTIN/UIN of Recipient','Receiver Name','Invoice Number','Invoice Date','Invoice Value','Place Of Supply','Reverse Charge','Applicable % of Tax Rate','Invoice Type','E-Commerce GSTIN','Rate','Taxable Value','Cess Amount']
    if not b2b_df.empty:
        for c in desired:
            if c not in b2b_df.columns: b2b_df[c] = ""
        b2b_df = b2b_df[desired]
    st.dataframe(b2b_df, use_container_width=True)
with col4:
    st.markdown("#### HSN (B2B)")
    hsn_b2b_df = get_compiled_data(st.session_state.global_data["hsn_b2b"])
    st.dataframe(hsn_b2b_df, use_container_width=True)

# ------------------------------
# Downloads
# ------------------------------
st.markdown("## Download Reports")

def df_download_button(label, df, filename_base, sheet="Sheet1"):
    if df.empty:
        st.button(label, disabled=True)
    else:
        data = to_excel_bytes(df, sheet_name=sheet)
        st.download_button(label, data=data, file_name=f"{filename_base}_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

b2cs_out = b2cs_df.rename(columns={'State':'Place Of Supply','GST Rate':'Rate'}).copy() if not b2cs_df.empty else pd.DataFrame()
if not b2cs_out.empty:
    b2cs_out['Type'] = 'OE'; b2cs_out['Applicable % of Tax Rate']=''; b2cs_out['Cess Amount']=0; b2cs_out['E-Commerce GSTIN']=''
    b2cs_out = b2cs_out[['Type','Place Of Supply','Rate','Applicable % of Tax Rate','Taxable Value','Cess Amount','E-Commerce GSTIN']]

df_download_button("Download Merged B2CS", b2cs_out if not b2cs_out.empty else pd.DataFrame(), "B2CS")

hsn_sales_out = hsn_df.rename(columns={'GST Rate':'Rate','Quantity':'Total Quantity','Invoice Amount':'Total Value','IGST':'Integrated Tax Amount','CGST':'Central Tax Amount','SGST':'State/UT Tax Amount'}).copy() if not hsn_df.empty else pd.DataFrame()
if not hsn_sales_out.empty:
    if 'Description' not in hsn_sales_out.columns: hsn_sales_out['Description']=''
    hsn_sales_out['UQC']='PCS-PIECES'
    if 'Cess amount' not in hsn_sales_out.columns: hsn_sales_out['Cess amount']=0
    desired_cols = ['HSN','Description','UQC','Total Quantity','Total Value','Taxable Value','Integrated Tax Amount','Central Tax Amount','State/UT Tax Amount','Cess amount','Rate']
    for c in desired_cols:
        if c not in hsn_sales_out.columns: hsn_sales_out[c] = 0 if c in ['Total Quantity','Total Value','Taxable Value','Integrated Tax Amount','Central Tax Amount','State/UT Tax Amount','Rate'] else ''
    hsn_sales_out = hsn_sales_out[desired_cols]

df_download_button("Download Merged HSN (Sales)", hsn_sales_out if not hsn_sales_out.empty else pd.DataFrame(), "hsn(b2c)")

df_download_button("Download B2B Output", b2b_df if not b2b_df.empty else pd.DataFrame(), "B2B_Output")

# Normalize HSN B2B export columns
hsn_b2b_out = hsn_b2b_df.copy()
if not hsn_b2b_out.empty:
    if 'Description' not in hsn_b2b_out.columns: hsn_b2b_out['Description']=''
    if 'UQC' not in hsn_b2b_out.columns: hsn_b2b_out['UQC']='PCS-PIECES'
    if 'GST Rate' in hsn_b2b_out.columns and 'Rate' not in hsn_b2b_out.columns: hsn_b2b_out = hsn_b2b_out.rename(columns={'GST Rate':'Rate'})
    if 'Quantity' in hsn_b2b_out.columns and 'Total Quantity' not in hsn_b2b_out.columns: hsn_b2b_out = hsn_b2b_out.rename(columns={'Quantity':'Total Quantity'})
    if 'Invoice Amount' in hsn_b2b_out.columns and 'Total Value' not in hsn_b2b_out.columns: hsn_b2b_out = hsn_b2b_out.rename(columns={'Invoice Amount':'Total Value'})
    if 'Integrated Tax Amount' not in hsn_b2b_out.columns and 'IGST' in hsn_b2b_out.columns: hsn_b2b_out = hsn_b2b_out.rename(columns={'IGST':'Integrated Tax Amount'})
    if 'Central Tax Amount' not in hsn_b2b_out.columns and 'CGST' in hsn_b2b_out.columns: hsn_b2b_out = hsn_b2b_out.rename(columns={'CGST':'Central Tax Amount'})
    if 'State/UT Tax Amount' not in hsn_b2b_out.columns and 'SGST' in hsn_b2b_out.columns: hsn_b2b_out = hsn_b2b_out.rename(columns={'SGST':'State/UT Tax Amount'})
    if 'Cess amount' not in hsn_b2b_out.columns: hsn_b2b_out['Cess amount']=0
    desired_cols = ['HSN','Description','UQC','Total Quantity','Total Value','Taxable Value','Integrated Tax Amount','Central Tax Amount','State/UT Tax Amount','Cess amount','Rate']
    for c in desired_cols:
        if c not in hsn_b2b_out.columns: hsn_b2b_out[c] = 0 if c in ['Total Quantity','Total Value','Taxable Value','Integrated Tax Amount','Central Tax Amount','State/UT Tax Amount','Rate'] else ''
    hsn_b2b_out = hsn_b2b_out[desired_cols]

df_download_button("Download HSN (B2B) Output", hsn_b2b_out if not hsn_b2b_out.empty else pd.DataFrame(), "hsn(b2b)")

# ECO TCS Report
def build_eco_tcs():
    report_data = []
    all_hsn = _concat_if_any(st.session_state.global_data["hsn"])
    if not all_hsn.empty:
        for col in ['Taxable Value','IGST','CGST','SGST']:
            if col in all_hsn.columns:
                all_hsn[col] = pd.to_numeric(all_hsn[col], errors='coerce').fillna(0)
    all_hsn_b2b = _concat_if_any(st.session_state.global_data["hsn_b2b"])
    if not all_hsn_b2b.empty:
        for col in ['Taxable Value','Integrated Tax Amount','Central Tax Amount','State/UT Tax Amount']:
            if col in all_hsn_b2b.columns:
                all_hsn_b2b[col] = pd.to_numeric(all_hsn_b2b[col], errors='coerce').fillna(0)

    eco_ops = ["Amazon","Flipkart","Jiomart","Meesho","Glowroad"]
    for op in eco_ops:
        gstin = st.session_state.eco_gstins.get(op, "")
        net_val = 0.0; igst=0.0; cgst=0.0; sgst=0.0
        if op=="Amazon":
            b2c = all_hsn[all_hsn.get('Source_Platform','')=='Amazon'] if not all_hsn.empty else pd.DataFrame()
            b2b = all_hsn_b2b[all_hsn_b2b.get('Source_Platform','')=='Amazon_B2B'] if not all_hsn_b2b.empty else pd.DataFrame()
            net_val += b2c.get('Taxable Value', pd.Series(dtype=float)).sum()
            igst += b2c.get('IGST', pd.Series(dtype=float)).sum()
            cgst += b2c.get('CGST', pd.Series(dtype=float)).sum()
            sgst += b2c.get('SGST', pd.Series(dtype=float)).sum()
            net_val += b2b.get('Taxable Value', pd.Series(dtype=float)).sum()
            igst += b2b.get('Integrated Tax Amount', pd.Series(dtype=float)).sum()
            cgst += b2b.get('Central Tax Amount', pd.Series(dtype=float)).sum()
            sgst += b2b.get('State/UT Tax Amount', pd.Series(dtype=float)).sum()
        elif op=="Flipkart":
            b2c = all_hsn[all_hsn.get('Source_Platform','')=='Flipkart'] if not all_hsn.empty else pd.DataFrame()
            net_val += b2c.get('Taxable Value', pd.Series(dtype=float)).sum()
            igst += b2c.get('IGST', pd.Series(dtype=float)).sum()
            cgst += b2c.get('CGST', pd.Series(dtype=float)).sum()
            sgst += b2c.get('SGST', pd.Series(dtype=float)).sum()
        elif op=="Jiomart":
            b2c = all_hsn[all_hsn.get('Source_Platform','')=='Jiomart'] if not all_hsn.empty else pd.DataFrame()
            net_val += b2c.get('Taxable Value', pd.Series(dtype=float)).sum()
            igst += b2c.get('IGST', pd.Series(dtype=float)).sum()
            cgst += b2c.get('CGST', pd.Series(dtype=float)).sum()
            sgst += b2c.get('SGST', pd.Series(dtype=float)).sum()
        elif op=="Meesho":
            s = all_hsn[all_hsn.get('Source_Platform','')=='Meesho_Sales'] if not all_hsn.empty else pd.DataFrame()
            r = all_hsn[all_hsn.get('Source_Platform','')=='Meesho_Return'] if not all_hsn.empty else pd.DataFrame()
            net_val += s.get('Taxable Value', pd.Series(dtype=float)).sum() + r.get('Taxable Value', pd.Series(dtype=float)).sum()
            igst += s.get('IGST', pd.Series(dtype=float)).sum() + r.get('IGST', pd.Series(dtype=float)).sum()
            cgst += s.get('CGST', pd.Series(dtype=float)).sum() + r.get('CGST', pd.Series(dtype=float)).sum()
            sgst += s.get('SGST', pd.Series(dtype=float)).sum() + r.get('SGST', pd.Series(dtype=float)).sum()
        elif op=="Glowroad":
            b2c = all_hsn[all_hsn.get('Source_Platform','')=='Glowroad'] if not all_hsn.empty else pd.DataFrame()
            net_val += b2c.get('Taxable Value', pd.Series(dtype=float)).sum()
            igst += b2c.get('IGST', pd.Series(dtype=float)).sum()
            cgst += b2c.get('CGST', pd.Series(dtype=float)).sum()
            sgst += b2c.get('SGST', pd.Series(dtype=float)).sum()

        if gstin or any(v!=0 for v in [net_val, igst, cgst, sgst]):
            report_data.append({
                'Nature of Supply': 'Liable to collect tax u/s 52(TCS)',
                'GSTIN of E-Commerce Operator': gstin,
                'E-Commerce Operator Name': op,
                'Net value of supplies': net_val,
                'Integrated tax': igst,
                'Central tax': cgst,
                'State/UT tax': sgst,
                'Cess': 0
            })
    return pd.DataFrame(report_data)[['Nature of Supply','GSTIN of E-Commerce Operator','E-Commerce Operator Name','Net value of supplies','Integrated tax','Central tax','State/UT tax','Cess']] if report_data else pd.DataFrame()

eco_tcs_df = build_eco_tcs()
def to_excel_bytes(df, sheet_name="Sheet1"):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
    return output.getvalue()

def df_download_button(label, df, filename_base, sheet="Sheet1"):
    if df.empty:
        st.button(label, disabled=True)
    else:
        data = to_excel_bytes(df, sheet_name=sheet)
        st.download_button(label, data=data, file_name=f"{filename_base}_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

st.markdown("## Download Reports")
b2cs_out = st.session_state.get('b2cs_out', pd.DataFrame())
hsn_df = st.session_state.get('hsn_df', pd.DataFrame())
b2b_df = st.session_state.get('b2b_df', pd.DataFrame())
hsn_b2b_df = st.session_state.get('hsn_b2b_df', pd.DataFrame())

