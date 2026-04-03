import streamlit as st
import pandas as pd
import requests

API_URL = "http://model_service:8000"

COUNTRIES = [
    "US", "UK", "CA", "FR", "DE", "JP", "AU", "IN", "BR", "MX",
    "IT", "ES", "NL", "SE", "NO", "DK", "FI", "CH", "AT", "BE",
    "PT", "IE", "NZ", "SG", "HK", "KR", "TW", "CN", "RU", "ZA",
]

MERCHANT_CATEGORIES = [
    "electronics", "fashion", "travel", "grocery", "gaming",
]

CHANNELS = ["web", "mobile", "in_store", "phone"]

st.set_page_config(page_title="Prediction", layout="wide")
st.header("Prediction")

tab1, tab2 = st.tabs(["Single Prediction", "Batch Prediction"])

# =========================
# SINGLE PREDICTION
# =========================
with tab1:
    st.subheader("Enter transaction details")
    col1, col2 = st.columns(2)

    with col1:
        amount = st.number_input("Amount", min_value=0.0, value=100.0)
        account_age_days = st.number_input(
            "Account Age (days)", min_value=0, value=365
        )
        shipping_distance_km = st.number_input(
            "Shipping Distance (km)", min_value=0.0, value=10.0
        )
        total_transactions_user = st.number_input(
            "Total Transactions", min_value=0, value=20
        )
        avg_amount_user = st.number_input(
            "Average Amount", min_value=0.0, value=80.0
        )
        transaction_hour = st.number_input(
            "Transaction Hour", min_value=0, max_value=23, value=14
        )
        transaction_day = st.number_input(
            "Transaction Day", min_value=0, max_value=6, value=2
        )

    with col2:
        country = st.selectbox("Country", COUNTRIES, index=0)
        bin_country = st.selectbox("BIN Country", COUNTRIES, index=0)
        merchant_category = st.selectbox(
            "Merchant Category", MERCHANT_CATEGORIES, index=0
        )
        channel = st.selectbox("Channel", CHANNELS, index=0)
        promo_used = st.selectbox("Promo Used", [0, 1])
        avs_match = st.selectbox("AVS Match", [0, 1])
        three_ds_flag = st.selectbox("3DS Used", [0, 1])
        cvv_result = st.selectbox("CVV Match", [0, 1])

    if st.button("Predict", key="single_predict"):
        payload = {
            "features": [{
                "amount": amount,
                "account_age_days": account_age_days,
                "shipping_distance_km": shipping_distance_km,
                "total_transactions_user": total_transactions_user,
                "avg_amount_user": avg_amount_user,
                "transaction_hour": transaction_hour,
                "transaction_day": transaction_day,
                "promo_used": promo_used,
                "avs_match": avs_match,
                "three_ds_flag": three_ds_flag,
                "cvv_result": cvv_result,
                "country": country,
                "bin_country": bin_country,
                "merchant_category": merchant_category,
                "channel": channel,
            }],
            "source": "webapp",
        }

        try:
            response = requests.post(
                f"{API_URL}/predict", json=payload, timeout=10
            )
            response.raise_for_status()

            data = response.json()
            result = data["results"][0]

            # Display prediction alongside features
            result_df = pd.DataFrame([{
                **payload["features"][0],
                "prediction": result["prediction"],
                "probability": round(result["probability"], 4),
            }])

            if result["prediction"] == 1:
                st.error(
                    f"FRAUD DETECTED (probability: {result['probability']:.2%})"
                )
            else:
                st.success(
                    f"Transaction is SAFE (probability: {result['probability']:.2%})"
                )

            st.dataframe(result_df, use_container_width=True)

        except requests.exceptions.ConnectionError:
            st.error("API not reachable. Is FastAPI running?")
        except Exception as e:
            st.error(f"Error: {e}")


# =========================
# BATCH PREDICTION
# =========================
with tab2:
    st.subheader("Upload a CSV file with transaction data")
    st.caption(
        "The CSV should contain these columns: amount, account_age_days, "
        "shipping_distance_km, total_transactions_user, avg_amount_user, "
        "transaction_hour, transaction_day, promo_used, avs_match, "
        "three_ds_flag, cvv_result, country, bin_country, "
        "merchant_category, channel"
    )

    uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])

    if uploaded_file:
        df = pd.read_csv(uploaded_file)
        st.write(f"Uploaded {len(df)} rows")
        st.dataframe(df.head(), use_container_width=True)

        if st.button("Run Batch Prediction", key="batch_predict"):
            try:
                payload = {
                    "features": df.to_dict(orient="records"),
                    "source": "webapp",
                }

                response = requests.post(
                    f"{API_URL}/predict", json=payload, timeout=30
                )
                response.raise_for_status()

                results = response.json()["results"]
                result_df = pd.DataFrame(results)

                # Show features + predictions together
                final_df = pd.concat(
                    [df.reset_index(drop=True), result_df], axis=1
                )
                st.success(f"Batch prediction completed ({len(results)} rows)")
                st.dataframe(final_df, use_container_width=True)

            except requests.exceptions.ConnectionError:
                st.error("API not reachable. Is FastAPI running?")
            except Exception as e:
                st.error(f"Error: {e}")
