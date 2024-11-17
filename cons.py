import requests
import folium
import webbrowser
import os
from math import radians, cos, sin, asin, sqrt
from datetime import datetime
from typing import Dict, Optional, Tuple
from kafka import KafkaConsumer
import json


class TrafficRouter:

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.tomtom.com/routing/1"

    def get_route_details(self, start_lat: float, start_lon: float, end_lat: float, end_lon: float) -> Dict:
        """Get detailed route information including traffic data"""
        url = f"{self.base_url}/calculateRoute/{start_lat},{start_lon}:{end_lat},{end_lon}/json"
        params = {'key': self.api_key, 'traffic': 'true', 'travelMode': 'car'}
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API request failed: {str(e)}")
            return None


def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two points in meters"""
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return 6371000 * c  # Earth radius in meters


def create_route_map_with_details(api_key: str, start: Tuple[float, float], end: Tuple[float, float]) -> Optional[str]:
    """Create an interactive map with detailed route information"""
    router = TrafficRouter(api_key)
    print("Fetching route details...")
    current_route = router.get_route_details(start_lat=start[0], start_lon=start[1], end_lat=end[0], end_lon=end[1])

    if not current_route or 'routes' not in current_route:
        print("Failed to get valid route details")
        return None

    center_lat, center_lon = (start[0] + end[0]) / 2, (start[1] + end[1]) / 2
    m = folium.Map(location=[center_lat, center_lon], zoom_start=10)
    route_points = [[pt['latitude'], pt['longitude']] for leg in current_route['routes'][0]['legs'] for pt in
                    leg['points']]

    folium.Marker(start, popup='Start: Restaurant Location', icon=folium.Icon(color='green')).add_to(m)
    folium.Marker(end, popup='End: Producer Location', icon=folium.Icon(color='red')).add_to(m)

    route_line = folium.PolyLine(route_points, weight=4, color='blue', opacity=0.8)
    route_line.add_to(m)

    output_file = 'route_map_detailed.html'
    m.save(output_file)
    return os.path.abspath(output_file)


def main():
    api_key = "rGjfNATy1VfWlOaZkEu4pAgilC76k5uI"
    consumer = KafkaConsumer('winetrade', 'winerestaurant', bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='earliest')

    start_coordinates, end_coordinates = None, None

    print("Listening for messages on Kafka topics...")
    for message in consumer:

        data = json.loads(message.value)

        if message.topic == 'winerestaurant':
            start_coordinates = (float(data['latitude']), float(data['longitude'].replace(',', '.')))

            print("Received restaurant coordinates:", start_coordinates)

        elif message.topic == 'winetrade':
            end_coordinates = (data['producer_latitude'], data['producer_longitude'])
            print("Received producer coordinates:", end_coordinates)

        if start_coordinates and end_coordinates:
            print("Creating route visualization...")
            map_file = create_route_map_with_details(api_key, start_coordinates, end_coordinates)

            if map_file:
                print("\nMap created successfully!")
                print("Opening in your default web browser...")
                webbrowser.open('file://' + map_file)
                print("Waiting for the next message...\n")

            # Reset coordinates to process the next message pair
            start_coordinates, end_coordinates = None, None


if __name__ == "__main__":
    main()

# import requests
# import folium
# import webbrowser
# import os
# from math import radians, cos, sin, asin, sqrt
# from typing import Dict, Tuple, Optional
# from kafka import KafkaConsumer
# import json
#
#
# class TrafficRouter:
#     def __init__(self, api_key: str):
#         self.api_key = api_key
#         self.base_url = "https://api.tomtom.com/routing/1"
#
#     def get_route_details(self, start_lat: float, start_lon: float, end_lat: float, end_lon: float) -> Dict:
#         """Get detailed route information including traffic data"""
#         url = f"{self.base_url}/calculateRoute/{start_lat},{start_lon}:{end_lat},{end_lon}/json"
#
#         params = {
#             'key': self.api_key,
#             'traffic': 'true',
#             'travelMode': 'car'
#         }
#
#         try:
#             response = requests.get(url, params=params)
#             response.raise_for_status()
#             return response.json()
#         except requests.exceptions.RequestException as e:
#             print(f"API request failed: {str(e)}")
#             return None
#
#
# def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
#     """Calculate distance between two points in meters"""
#     lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
#     dlat = lat2 - lat1
#     dlon = lon2 - lon1
#     a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
#     c = 2 * asin(sqrt(a))
#     r = 6371000  # Radius of Earth in meters
#     return c * r
#
#
# def create_route_map_with_details(api_key: str, start: Tuple[float, float], end: Tuple[float, float]) -> Optional[str]:
#     """Create an interactive map with detailed route information"""
#     router = TrafficRouter(api_key)
#
#     print("Fetching route details...")
#     current_route = router.get_route_details(
#         start_lat=start[0],
#         start_lon=start[1],
#         end_lat=end[0],
#         end_lon=end[1]
#     )
#
#     if not current_route or 'routes' not in current_route or not current_route['routes']:
#         print("Failed to get valid route details")
#         return None
#
#     # Calculate center point for the map
#     center_lat = (start[0] + end[0]) / 2
#     center_lon = (start[1] + end[1]) / 2
#
#     # Create map
#     m = folium.Map(location=[center_lat, center_lon], zoom_start=10)
#
#     # Extract route information
#     route = current_route['routes'][0]
#     summary = route['summary']
#
#     # Extract route points
#     route_points = []
#     for leg in route['legs']:
#         for point in leg['points']:
#             route_points.append([point['latitude'], point['longitude']])
#
#     # Add start and end markers
#     folium.Marker(start, popup='Start: Restaurant Location', icon=folium.Icon(color='green')).add_to(m)
#     folium.Marker(end, popup='End: Producer location', icon=folium.Icon(color='red')).add_to(m)
#
#     # Calculate route metrics
#     distance_km = summary['lengthInMeters'] / 1000
#     duration_min = summary['travelTimeInSeconds'] / 60
#     delay_min = summary.get('trafficDelayInSeconds', 0) / 60
#     avg_speed = (distance_km / (duration_min / 60))
#
#     # Create route line with popup
#     route_line = folium.PolyLine(route_points, weight=4, color='blue', opacity=0.8,
#                                  popup=f"<b>Distance:</b> {distance_km:.1f} km<br>"
#                                        f"<b>Time:</b> {int(duration_min)} min<br>"
#                                        f"<b>Delay:</b> {int(delay_min)} min<br>"
#                                        f"<b>Speed:</b> {avg_speed:.1f} km/h")
#     route_line.add_to(m)
#
#     # Save map
#     output_file = 'route_map_detailed.html'
#     m.save(output_file)
#     return os.path.abspath(output_file)
#
#
# def main():
#     api_key = "rGjfNATy1VfWlOaZkEu4pAgilC76k5uI"
#
#     # Create Kafka consumers
#     consumer = KafkaConsumer(
#         'winetrade', 'winerestaurant',
#         bootstrap_servers='localhost:9092',
#         value_deserializer=lambda x: json.loads(x.decode('utf-8'))
#     )
#
#     start_coordinates = None
#     end_coordinates = None
#
#     for message in consumer:
#         topic = message.topic
#         data = message.value
#
#         if topic == 'winetrade':
#             start_coordinates = (data['producer_latitude'], data['producer_longitude'])
#             print("Received Producer Message:", data)
#
#         elif topic == 'winerestaurant':
#             end_coordinates = (float(data['latitude']), float(data['longitude'].replace(',', '.')))
#             print("Received Restaurant Message:", data)
#
#         # Once we have both start and end coordinates, create the map
#         if start_coordinates and end_coordinates:
#             print("Creating route visualization...")
#             map_file = create_route_map_with_details(api_key, start_coordinates, end_coordinates)
#
#             if map_file:
#                 print("\nMap created successfully!")
#                 print("Opening in your default web browser...")
#                 webbrowser.open('file://' + map_file)
#             else:
#                 print("Failed to create map. Please check your internet connection and API key.")
#             break  # Stop after processing one pair of messages
#
#
# if __name__ == "__main__":
#     main()

# from confluent_kafka import Consumer, KafkaError
# import json
#
#
# class KafkaConsumerService:
#     def __init__(self, broker, group_id, topics):
#         self.consumer = Consumer({
#             'bootstrap.servers': broker,
#             'group.id': group_id,
#             'auto.offset.reset': 'earliest'
#         })
#         self.topics = topics
#         self.consumer.subscribe(self.topics)
#         print(f"Subscribed to topics: {self.topics}")
#
#     def consume_messages(self):
#         try:
#             while True:
#                 msg = self.consumer.poll(1.0)  # Poll for messages
#                 if msg is None:
#                     continue
#                 if msg.error():
#                     if msg.error().code() == KafkaError._PARTITION_EOF:
#                         print(f"End of partition reached {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
#                     else:
#                         print(f"Error: {msg.error()}")
#                     continue
#
#                 # Process the message
#                 message = json.loads(msg.value().decode('utf-8'))
#                 topic = msg.topic()
#                 print(f"Received message from topic {topic}: {message}")
#
#                 if topic == 'winetrade':
#                     self.process_producer_message(message)
#                 elif topic == 'winerestaurant':
#                     self.process_restaurant_message(message)
#
#         except Exception as e:
#             print(f"Error while consuming messages: {str(e)}")
#         finally:
#             # Close the consumer when done
#             self.consumer.close()
#
#     def process_producer_message(self, message):
#         # Implement processing logic for producer message
#         print(f"Processing producer message: {message}")
#
#     def process_restaurant_message(self, message):
#         # Implement processing logic for restaurant message
#         print(f"Processing restaurant message: {message}")
#
#
# # Usage example
# if __name__ == "__main__":
#     kafka_consumer = KafkaConsumerService(
#         broker='localhost:9092',
#         group_id='wine-consumer-group',
#         topics=['winetrade', 'winerestaurant']
#     )
#     kafka_consumer.consume_messages()
