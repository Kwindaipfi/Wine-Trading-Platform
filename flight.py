import requests
from datetime import datetime, timedelta
from kafka import KafkaConsumer
import json


# Function to get today's date in the required format (YYYY-MM-DD)
def get_today_date():
    return datetime.today().strftime('%Y-%m-%d')


def create_html_start():
    """Create the start of HTML document"""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Flight Search Results</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            .flight { border: 1px solid #ccc; margin: 20px 0; padding: 15px; }
            .itinerary { margin: 15px 0; padding: 10px; background: #f5f5f5; }
            .segment { margin: 10px 0; padding: 10px; border-left: 3px solid #007bff; }
            .price { font-size: 1.2em; color: #28a745; }
            .header { font-weight: bold; color: #007bff; }
        </style>
    </head>
    <body>
        <h1>Flight Search Results</h1>
    """


def create_html_end():
    """Create the end of HTML document"""
    return """
    </body>
    </html>
    """


def get_access_token(api_key, api_secret):
    """Get OAuth 2.0 access token from Amadeus"""
    token_url = "https://test.api.amadeus.com/v1/security/oauth2/token"

    data = {
        "grant_type": "client_credentials",
        "client_id": api_key,
        "client_secret": api_secret
    }

    response = requests.post(token_url, data=data)

    if response.status_code == 200:
        return response.json()["access_token"]
    else:
        print(f"Error getting access token: {response.status_code} - {response.text}")
        return None


def format_duration(duration):
    """Convert PT2H30M format to '2h 30m' format"""
    hours = minutes = 0
    if 'H' in duration:
        hours = int(duration.split('H')[0].replace('PT', ''))
    if 'M' in duration:
        minutes = int(duration.split('H')[-1].replace('M', ''))
    return f"{hours}h {minutes}m"


def parse_duration(duration):
    """Parse duration in PT2H30M format to total minutes for comparison"""
    hours = minutes = 0
    if 'H' in duration:
        hours = int(duration.split('H')[0].replace('PT', ''))
    if 'M' in duration:
        minutes = int(duration.split('H')[-1].replace('M', ''))
    return hours * 60 + minutes


def format_datetime(datetime_str):
    """Convert datetime string to more readable format"""
    dt = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
    return dt.strftime("%Y-%m-%d %I:%M %p")


def get_flight_data(access_token, origin, destination, departure_date):
    """Get flight offers using the access token"""
    url = "https://test.api.amadeus.com/v2/shopping/flight-offers"

    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    params = {
        "originLocationCode": origin,
        "destinationLocationCode": destination,
        "departureDate": departure_date,
        "adults": 1,
        "max": 30,
        "currencyCode": "USD"
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching flight data: {response.status_code} - {response.text}")
        return None


def convert_to_zar(amount, from_currency):
    """Convert the amount to ZAR"""
    conversion_url = f"https://api.exchangerate-api.com/v4/latest/{from_currency}"
    response = requests.get(conversion_url)

    if response.status_code == 200:
        rates = response.json()["rates"]
        zar_rate = rates.get("ZAR")
        if zar_rate:
            return amount * zar_rate
    return amount


def optimize_flights(flight_data, optimization_criteria='price', restaurant_opening_time='10:00'):
    """Filter and sort flights based on optimization criteria and restaurant opening time"""
    if not flight_data or 'data' not in flight_data:
        print("No flight data available")
        return []

    flights = []
    restaurant_open_time = datetime.strptime(restaurant_opening_time, '%H:%M')

    for offer in flight_data['data']:
        price = float(offer['price']['total'])
        currency = offer['price']['currency']
        price_zar = convert_to_zar(price, currency) if currency != "ZAR" else price
        itinerary = offer['itineraries'][0]
        duration = parse_duration(itinerary['duration'])

        arrival_time = datetime.fromisoformat(itinerary['segments'][-1]['arrival']['at'].replace('Z', '+00:00'))
        arrival_window = arrival_time.replace(hour=restaurant_open_time.hour,
                                              minute=restaurant_open_time.minute) - timedelta(hours=1)

        if arrival_time <= arrival_window:
            flights.append({
                'price': price,
                'price_zar': price_zar,
                'duration': duration,
                'currency': currency,
                'offer': offer
            })

    if optimization_criteria == 'price':
        flights.sort(key=lambda x: x['price'])
    elif optimization_criteria == 'duration':
        flights.sort(key=lambda x: x['duration'])
    elif optimization_criteria == 'best_value':
        flights.sort(key=lambda x: (x['price'] * 0.5 + x['duration'] * 0.5))

    return flights


def flight_to_html(flight):
    """Convert flight details to HTML format"""
    offer = flight['offer']
    price = flight['price']
    price_zar = flight['price_zar']
    currency = flight['currency']

    html = f"""
    <div class="flight">
        <div class="price">Total Price: {currency} {price:.2f} (ZAR {price_zar:.2f})</div>
    """

    for itinerary_idx, itinerary in enumerate(offer['itineraries'], 1):
        html += f"""
        <div class="itinerary">
            <div class="header">Itinerary {itinerary_idx}</div>
        """

        for segment_idx, segment in enumerate(itinerary['segments'], 1):
            carrier_code = segment['carrierCode']
            flight_number = segment['number']
            dep_airport = segment['departure']['iataCode']
            dep_terminal = segment['departure'].get('terminal', 'N/A')
            dep_time = format_datetime(segment['departure']['at'])
            arr_airport = segment['arrival']['iataCode']
            arr_terminal = segment['arrival'].get('terminal', 'N/A')
            arr_time = format_datetime(segment['arrival']['at'])
            duration = format_duration(segment['duration'])
            aircraft = segment.get('aircraft', {}).get('code', 'N/A')
            seats = segment.get('numberOfStops', 0)
            cabin_class = offer['travelerPricings'][0]['fareDetailsBySegment'][segment_idx - 1]['cabin']

            html += f"""
            <div class="segment">
                <p>Flight: {carrier_code} {flight_number}</p>
                <p>Departure: {dep_airport} Terminal {dep_terminal}</p>
                <p>Departure Time: {dep_time}</p>
                <p>Arrival: {arr_airport} Terminal {arr_terminal}</p>
                <p>Arrival Time: {arr_time}</p>
                <p>Flight Duration: {duration}</p>
                <p>Aircraft: {aircraft}</p>
                <p>Stops: {seats}</p>
                <p>Cabin Class: {cabin_class}</p>
            </div>
            """

        html += "</div>"

    html += "</div>"
    return html


def display_flight_details(flight):
    """Display details of a single flight offer"""
    offer = flight['offer']
    price = flight['price']
    price_zar = flight['price_zar']
    currency = flight['currency']
    print(f"\n{'=' * 80}")
    print(f"Total Price: {currency} {price:.2f} (ZAR {price_zar:.2f})")

    for itinerary_idx, itinerary in enumerate(offer['itineraries'], 1):
        print(f"\nItinerary {itinerary_idx}:")
        print(f"{'-' * 40}")

        for segment_idx, segment in enumerate(itinerary['segments'], 1):
            carrier_code = segment['carrierCode']
            flight_number = segment['number']
            print(f"\nFlight: {carrier_code} {flight_number}")

            dep_airport = segment['departure']['iataCode']
            dep_terminal = segment['departure'].get('terminal', 'N/A')
            dep_time = format_datetime(segment['departure']['at'])
            print(f"Departure: {dep_airport} Terminal {dep_terminal}")
            print(f"Departure Time: {dep_time}")

            arr_airport = segment['arrival']['iataCode']
            arr_terminal = segment['arrival'].get('terminal', 'N/A')
            arr_time = format_datetime(segment['arrival']['at'])
            print(f"Arrival: {arr_airport} Terminal {arr_terminal}")
            print(f"Arrival Time: {arr_time}")

            duration = format_duration(segment['duration'])
            print(f"Flight Duration: {duration}")

            aircraft = segment.get('aircraft', {}).get('code', 'N/A')
            print(f"Aircraft: {aircraft}")

            seats = segment.get('numberOfStops', 0)
            print(f"Stops: {seats}")

            cabin_class = offer['travelerPricings'][0]['fareDetailsBySegment'][segment_idx - 1]['cabin']
            print(f"Cabin Class: {cabin_class}")


def listen_for_kafka_messages():
    # Modified Kafka consumer configuration to read only new messages
    consumer = KafkaConsumer(
        'winetrade',
        'winerestaurant',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',  # Changed from 'earliest' to 'latest'
        enable_auto_commit=True,
        group_id='flight_search_group',  # Added group_id for consistent consumption
        consumer_timeout_ms=1000  # Added timeout to handle no new messages
    )

    origin = ''
    destination = ''

    # Create HTML file and write header
    with open('flight_result3.html', 'w', encoding='utf-8') as f:
        f.write(create_html_start())

    try:
        while True:  # Continuous loop to keep listening
            try:
                message_batch = consumer.poll(timeout_ms=1000)  # Poll for new messages

                if not message_batch:
                    continue  # If no new messages, continue waiting

                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        api_key = 'C3oBu3toXbjRby47AkLaDZ86gaCLPkyK'
                        api_secret = 'jRGr8X076xshHF0G'

                        data = json.loads(message.value)

                        if message.topic == 'winerestaurant':
                            origin = data.get('airport_code')
                            print(f"Received restaurant location: {origin}")
                        elif message.topic == 'winetrade':
                            destination = data.get('producer_airport_code')
                            print(f"Received trade destination: {destination}")

                        print(f"Received Kafka message: {data}")
                        departure_date = data.get('departure_date', '2024-11-16')
                        restaurant_opening_time = data.get('Opening_Hours', '10:00')

                        # Only proceed if we have both origin and destination
                        if origin and destination:
                            access_token = get_access_token(api_key, api_secret)

                            if access_token:
                                flight_data = get_flight_data(access_token, origin, destination, departure_date)

                                if flight_data:
                                    optimized_flights = optimize_flights(flight_data, optimization_criteria='price',
                                                                         restaurant_opening_time=restaurant_opening_time)

                                    # Write flights to HTML file
                                    with open('flight_result3.html', 'a', encoding='utf-8') as f:
                                        f.write(
                                            f"<h2>Search Results for {origin} to {destination} on {departure_date}</h2>")
                                        if optimized_flights:
                                            for flight in optimized_flights:
                                                f.write(flight_to_html(flight))
                                                display_flight_details(flight)
                                        else:
                                            f.write("<p>No flights found matching the criteria.</p>")

            except Exception as e:
                print(f"Error processing message: {e}")
                continue

    except KeyboardInterrupt:
        print("Shutting down gracefully...")
    finally:
        # Close HTML file
        with open('flight_result3.html', 'a', encoding='utf-8') as f:
            f.write(create_html_end())
        consumer.close()

if __name__ == "__main__":
    listen_for_kafka_messages()

