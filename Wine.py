from pyspark.sql import SparkSession
import json
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, jsonify, session
from confluent_kafka import Producer

# Initialize Flask app
app = Flask(__name__,
            static_folder='static',
            template_folder='templates')
app.secret_key = 'your-secret-key-here'  # Required for session management

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC_PRODUCER = 'winetrade'
KAFKA_TOPIC_RESTAURANT = 'winerestaurant'
KAFKA_TOPIC_AUCTION = 'wineauction'

# Kafka Producer configuration
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

# Initialize Spark session with the spark-excel package
spark = SparkSession.builder \
    .appName("Wine Data Analysis") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.7") \
    .getOrCreate()
# Define the file paths in HDFS
file_path_producers = "hdfs:///user/kwind/wine_data/Producers.xlsx"
file_path_restaurants = "hdfs:///user/kwind/wine_data/Restaurants.xlsx"

# Load Producers data from Excel
df_producers = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(file_path_producers)

# Rename columns in Producers DataFrame
df_producers = (df_producers
    .withColumnRenamed("name", "producer_name")
    .withColumnRenamed("location", "producer_location")
    .withColumnRenamed("address", "producer_address")
    .withColumnRenamed("P latitude", "producer_latitude")
    .withColumnRenamed("P longitude", "producer_longitude")
    .withColumnRenamed("P airport_latitude", "producer_airport_latitude")
    .withColumnRenamed("P airport_longitude", "producer_airport_longitude")
    .withColumnRenamed("airport_code", "producer_airport_code")
)

# Load Restaurants data from Excel
df_restaurants = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(file_path_restaurants)

print(df_restaurants.columns)
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def collect_demand(restaurants_df, restaurant_name, custom_budget):
    demands = []
    for row in restaurants_df.collect():
        if row['name'] == restaurant_name:
            wine_preferences = eval(row['Wine Preferences'])
            for wine_type, quantity in wine_preferences.items():
                demands.append({
                    'restaurant_name': row['name'],
                    'wine_type': wine_type,
                    'quantity': quantity,
                    'custom_budget': float(custom_budget),
                    **row.asDict()
                })
            break
    return pd.DataFrame(demands)


def collect_offers(producers_df):
    offers = []
    for row in producers_df.collect():
        offers.append({
            'producer_id': row['Producer_id'],
            'red_price': row['red_price'],
            'white_price': row['white_price'],
            'rose_price': row['rose_price'] if 'rose_price' in row else None,
            **row.asDict()
        })
    return pd.DataFrame(offers)


def find_best_offer_subset(offers, remaining_budget, wine_type, quantity):
    price_column = f'{wine_type.lower()}_price'
    sorted_offers = sorted(offers, key=lambda x: float(x[price_column]))
    best_offer_subset = []
    total_cost = 0
    quantity_per_producer = quantity // min(4, len(sorted_offers))  # Distribute quantity among producers

    for offer in sorted_offers[:4]:
        price = float(offer[price_column])
        subtotal = price * quantity_per_producer

        if total_cost + subtotal <= remaining_budget:
            offer['allocated_quantity'] = quantity_per_producer
            offer['subtotal_cost'] = subtotal
            best_offer_subset.append(offer)
            total_cost += subtotal

    return best_offer_subset, total_cost


def auction_match(demands_df, offers_df):
    allocations = []
    for _, demand in demands_df.iterrows():
        remaining_budget = float(demand['custom_budget'])
        wine_type = demand['wine_type']
        quantity = demand['quantity']

        price_column = f'{wine_type.lower()}_price'
        if price_column not in offers_df.columns:
            print(f"Warning: Price column for {wine_type} not found in producer offers.")
            continue

        relevant_offers = offers_df[offers_df[price_column].notnull()]
        best_offer_subset, total_cost = find_best_offer_subset(
            relevant_offers.to_dict('records'),
            remaining_budget,
            wine_type,
            quantity
        )

        if best_offer_subset:
            for offer in best_offer_subset:
                allocations.append({
                    'restaurant_name': demand['restaurant_name'],
                    'wine_type': wine_type,
                    'quantity': offer['allocated_quantity'],
                    'initial_budget': demand['custom_budget'],
                    'total_cost': offer['subtotal_cost'],
                    'remaining_budget': remaining_budget - total_cost,
                    'producer_id': offer['producer_id'],
                    'producer_name': offer['producer_name'],
                    'unit_price': float(offer[price_column]),
                    **{k: v for k, v in demand.items() if k not in ['quantity', 'custom_budget']},
                    **{k: v for k, v in offer.items() if
                       not k.startswith('allocated_') and not k.startswith('subtotal_')}
                })

    return pd.DataFrame(allocations)


@app.route('/select_producer/<producer_id>', methods=['POST'])
def select_producer(producer_id):
    # Add debugging prints
    print(f"Attempting to select producer with ID: {producer_id}")
    print(f"Session contents: {session}")

    allocations = session.get('allocations', [])
    restaurant_details = session.get('restaurant_details', {})

    print(f"Retrieved allocations: {allocations}")
    print(f"Retrieved restaurant details: {restaurant_details}")

    # Convert producer_id to the correct type for comparison
    producer_id = str(producer_id)  # Convert to string for consistent comparison

    # Find the selected producer's allocation with better error handling
    try:
        selected_allocation = next(
            (alloc for alloc in allocations
             if str(alloc.get('producer_id', '')) == producer_id),
            None
        )

        print(f"Selected allocation: {selected_allocation}")

        if not selected_allocation:
            raise ValueError(f"No allocation found for producer ID: {producer_id}")

        # Prepare producer message with error handling
        producer_message = {
            'producer_id': selected_allocation.get('producer_id'),
            'producer_name': selected_allocation.get('producer_name'),
            'producer_location': selected_allocation.get('producer_location'),
            'producer_latitude': selected_allocation.get('producer_latitude'),
            'producer_longitude': selected_allocation.get('producer_longitude'),
            'producer_airport_latitude': selected_allocation.get('producer_airport_latitude'),
            'producer_airport_longitude': selected_allocation.get('producer_airport_longitude'),
            'producer_airport_code': selected_allocation.get('producer_airport_code'),
            'wine_type': selected_allocation.get('wine_type'),
            'quantity': selected_allocation.get('quantity'),
            'unit_price': selected_allocation.get('unit_price'),
            'total_cost': selected_allocation.get('total_cost'),
            'timestamp': pd.Timestamp.now().isoformat()
        }

        # Prepare restaurant message with error handling
        restaurant_message = {
            'restaurant_name': restaurant_details.get('name'),
            'location': restaurant_details.get('location'),
            'latitude': restaurant_details.get('latitude'),
            'longitude': restaurant_details.get('longitude'),
            'airport_code': restaurant_details.get('Airport_code'),
            'Opening_Hours': restaurant_details.get('Opening Hours'),
            'timestamp': pd.Timestamp.now().isoformat()
        }


        # Validate messages before sending
        if not all(producer_message.values()):
            missing_fields = [k for k, v in producer_message.items() if not v]
            raise ValueError(f"Missing producer data for fields: {missing_fields}")

        if not all(restaurant_message.values()):
            missing_fields = [k for k, v in restaurant_message.items() if not v]
            raise ValueError(f"Missing restaurant data for fields: {missing_fields}")

        # Send messages to Kafka with error handling
        try:
            # Send producer details to Kafka
            producer.produce(
                KAFKA_TOPIC_PRODUCER,
                key=str(producer_message['producer_id']),
                value=json.dumps(producer_message),
                callback=delivery_report
            )

            # Send restaurant details to Kafka
            producer.produce(
                KAFKA_TOPIC_RESTAURANT,
                key=str(restaurant_message['restaurant_name']),
                value=json.dumps(restaurant_message),
                callback=delivery_report
            )

            producer.flush()

            # Store the selected details in session
            session['selected_producer'] = producer_message
            session['selected_restaurant'] = restaurant_message

            print("Successfully processed producer selection")
            return redirect(url_for('delivery_details'))

        except Exception as e:
            print(f"Kafka producer error: {str(e)}")
            return jsonify({
                'error': 'Failed to send messages to Kafka',
                'details': str(e)
            }), 500

    except Exception as e:
        print(f"Error in producer selection: {str(e)}")
        return jsonify({
            'error': 'Producer not found or invalid data',
            'details': str(e)
        }), 404


# Modify the index route to ensure proper session handling
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        restaurant_name = request.form['restaurant_name']
        custom_budget = request.form['custom_budget']

        # Clear existing session data
        session.clear()

        try:
            # Collect demand and offers
            demands_df = collect_demand(df_restaurants, restaurant_name, custom_budget)
            if demands_df.empty:
                return jsonify({
                    'error': 'Restaurant not found or no wine preferences specified'
                }), 404

            offers_df = collect_offers(df_producers)
            if offers_df.empty:
                return jsonify({
                    'error': 'No producer offers available'
                }), 404

            # Perform auction matching
            allocations_df = auction_match(demands_df, offers_df)

            # Convert DataFrames to dictionaries for session storage
            allocations_dict = allocations_df.to_dict(orient="records")
            restaurant_details = demands_df.iloc[0].to_dict()

            # Store in session
            session['allocations'] = allocations_dict
            session['restaurant_details'] = restaurant_details

            print(f"Stored in session - allocations: {session['allocations']}")
            print(f"Stored in session - restaurant details: {session['restaurant_details']}")

            # Calculate summary statistics
            summary_stats = {
                'unique_restaurants': len(allocations_df['restaurant_name'].unique()),
                'unique_producers': len(allocations_df['producer_name'].unique()),
                'total_cost': float(allocations_df['total_cost'].sum()),
                'total_quantity': int(allocations_df['quantity'].sum())
            }


            return render_template('results.html',
                                   allocations=allocations_dict,
                                   summary_stats=summary_stats)

        except Exception as e:
            print(f"Error in index route: {str(e)}")
            return jsonify({
                'error': 'Failed to process auction',
                'details': str(e)
            }), 500

    return render_template('index.html')

@app.route('/delivery_details')
def delivery_details():
    producer_details = session.get('selected_producer', {})
    restaurant_details = session.get('selected_restaurant', {})

    return render_template('delivery_details.html',
                           producer=producer_details,
                           restaurant=restaurant_details)


if __name__ == "__main__":
    app.run(debug=True)


